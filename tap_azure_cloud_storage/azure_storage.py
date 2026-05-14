# tap_azure_cloud_storage/azure_storage.py

import io
import json
import re
import gzip
import struct
import itertools
import singer
import backoff
import adlfs
from singer_encodings import (
    csv as singer_csv,
    jsonl as singer_jsonl,
    parquet as singer_parquet,
    avro as singer_avro,
    compression
)
from tap_azure_cloud_storage import conversion
from tap_azure_cloud_storage.exceptions import (
    AzureBackoffError,
    AzureRateLimitError,
    RAW_EXCEPTIONS,
    raise_for_error,
)

LOGGER = singer.get_logger()

MAX_TRIES = 5  # Maximum total attempts (initial + retries) for backoff decorator

# Global Azure filesystem instance for streaming Parquet/Avro files
fs = None

# Global counter for skipped files during discovery and sync
skipped_files_count = 0

SDC_SOURCE_CONTAINER_COLUMN = "_sdc_source_container"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_EXTRA_COLUMN = "_sdc_extra"


def unwrap_excel_commented_cells(row_dict):
    """Unwrap singer-encodings commented/hyperlinked cell values.

    singer_encodings wraps cells that carry a comment or hyperlink as::

        [{"text": <value>, "comment": {...}}]

    This helper extracts the plain ``text`` value so that downstream
    schema inference and the Singer Transformer see the real data type
    instead of a list.
    """
    unwrapped = {}
    for key, value in row_dict.items():
        if (
            isinstance(value, list)
            and len(value) == 1
            and isinstance(value[0], dict)
            and 'text' in value[0]
        ):
            unwrapped[key] = value[0]['text']
        else:
            unwrapped[key] = value
    return unwrapped

DEFAULT_SAMPLE_RATE = 5
DEFAULT_MAX_RECORDS = 1000
DEFAULT_MAX_FILES = 5

def _read_exact(fp, n):
    """Read exactly n bytes from file pointer.
    Helper function for reading gzip headers.
    """
    data = fp.read(n)
    while len(data) < n:
        b = fp.read(n - len(data))
        if not b:
            raise EOFError("Compressed file ended before the "
                           "end-of-stream marker was reached")
        data += b
    return data

def get_file_name_from_gzfile(filename=None, fileobj=None):
    """Read filename from gzip file header.
    Returns the original filename stored in the gzip header,
    or falls back to stripping .gz extension if not present.
    """
    _gz = gzip.GzipFile(filename=filename, fileobj=fileobj)
    _fp = _gz.fileobj

    # Check magic bytes: 0x1f 0x8b
    magic = _fp.read(2)
    if magic == b'':
        return None

    if magic != b'\037\213':
        raise OSError('Not a gzipped file (%r)' % magic)

    (method, flag, _) = struct.unpack("<BBIxx", _read_exact(_fp, 8))
    if method != 8:
        raise OSError('Unknown compression method')

    # Check if filename is stored in header
    if not flag & gzip.FNAME:
        # Not stored in the header, use the filename sans .gz
        fname = _fp.name if hasattr(_fp, 'name') else filename
        if fname:
            return fname[:-3] if fname.endswith('.gz') else fname
        return None

    if flag & gzip.FEXTRA:
        # Read & discard the extra field, if present
        extra_len, = struct.unpack("<H", _read_exact(_fp, 2))
        _read_exact(_fp, extra_len)

    _fname = []  # bytes for fname
    if flag & gzip.FNAME:
        # Read null-terminated string containing the filename
        # RFC 1952 specifies FNAME is encoded in latin1
        while True:
            s = _fp.read(1)
            if not s or s == b'\000':
                break
            _fname.append(s)
        return ''.join([s.decode('latin1') for s in _fname])

    return None

@backoff.on_exception(
    backoff.expo,
    AzureRateLimitError,
    max_tries=6,
    max_time=60,
    jitter=None,
)
@backoff.on_exception(
    backoff.expo,
    AzureBackoffError,
    max_tries=MAX_TRIES,
    factor=2,
)
def setup_azure_client(config):
    """
    Setup Azure Data Lake filesystem client using adlfs.
    Uses adlfs which provides a filesystem interface with random access support for all file types.
    Supports multiple authentication methods:
    - Service Principal (client_id, client_secret, tenant_id)
    - Connection String
    - Account Key
    - DefaultAzureCredential (for managed identity)
    """
    global fs
    if fs is None:
        try:
            storage_account_name = config.get('storage_account_name')

            # Build authentication parameters based on available credentials
            if config.get('client_id') and config.get('client_secret') and config.get('tenant_id'):
                LOGGER.info("Using Service Principal authentication")
                fs = adlfs.AzureBlobFileSystem(
                    account_name=storage_account_name,
                    tenant_id=config['tenant_id'],
                    client_id=config['client_id'],
                    client_secret=config['client_secret']
                )
            elif config.get('connection_string'):
                LOGGER.info("Using Connection String authentication")
                fs = adlfs.AzureBlobFileSystem(
                    connection_string=config['connection_string']
                )
            elif config.get('account_key'):
                LOGGER.info("Using Account Key authentication")
                fs = adlfs.AzureBlobFileSystem(
                    account_name=storage_account_name,
                    account_key=config['account_key']
                )
            else:
                LOGGER.info("Using Default Azure Credential (managed identity)")
                fs = adlfs.AzureBlobFileSystem(
                    account_name=storage_account_name
                )
        except RAW_EXCEPTIONS as e:
            raise_for_error(e)
        except Exception as e:
            raise Exception("Failed to create Azure filesystem client") from e
    return fs

@backoff.on_exception(
    backoff.expo,
    AzureRateLimitError,
    max_tries=6,
    max_time=60,
    jitter=None,
)
@backoff.on_exception(
    backoff.expo,
    AzureBackoffError,
    max_tries=MAX_TRIES,
    factor=2,
)
def _list_blobs_with_retry(fs_client, path):
    """Non-generator wrapper so backoff can retry the full blob listing."""
    try:
        return list(fs_client.find(path, detail=True).values())
    except RAW_EXCEPTIONS as e:
        raise_for_error(e)


def list_files_in_container(config):
    """
    Generator to list files in an Azure Blob Storage container.
    Used to validate connectivity & permissions.
    """
    fs_client = setup_azure_client(config)
    container_name = config.get("container_name")
    prefix = config.get("root_path", "")
    if not container_name:
        LOGGER.error("Container name not found in config")
        raise ValueError("Container name not found in config")

    try:
        # Strip leading slash from prefix to avoid double slashes
        prefix = prefix.lstrip('/') if prefix else ""
        # Build the path for adlfs
        path = f"{container_name}/{prefix}" if prefix else container_name

        # Use the backoff-enabled helper to retrieve the full blob listing
        # so that transient errors are retried before any blobs are yielded,
        # avoiding duplicates that would occur with a yield-then-retry pattern.
        files = _list_blobs_with_retry(fs_client, path)

        # Convert adlfs file info to blob-like objects
        for file_info in files:
            if file_info['type'] == 'file':
                # Create a simple object with blob-like attributes
                class BlobInfo:
                    def __init__(self, name, last_modified):
                        # Remove container prefix from name
                        self.name = name.replace(f"{container_name}/", "", 1) if name.startswith(f"{container_name}/") else name
                        self.last_modified = last_modified

                yield BlobInfo(file_info['name'], file_info.get('last_modified'))
    except (AzureBackoffError, AzureRateLimitError):
        # Retries are already exhausted by the backoff decorator; let the error propagate.
        raise
    except RAW_EXCEPTIONS as e:
        raise_for_error(e)
    except Exception as e:
        raise Exception("Failed to list files in Azure container") from e

@backoff.on_exception(
    backoff.expo,
    AzureRateLimitError,
    max_tries=6,
    max_time=60,
    jitter=None,
)
@backoff.on_exception(
    backoff.expo,
    AzureBackoffError,
    max_tries=MAX_TRIES,
    factor=2,
)
def get_file_handle(config, blob_path):
    """
    Get a streaming file handle for all file types using adlfs.
    This provides random access (seeking) support for formats that need it (Parquet/Avro)
    and works efficiently for sequential reading (CSV/JSON/Excel).
    Retries automatically on transient 5xx / 429 errors.
    """
    try:
        container_name = config['container_name']
        fs_client = setup_azure_client(config)
        # Open file with adlfs - supports streaming with random access
        return fs_client.open(f'{container_name}/{blob_path}', 'rb')
    except RAW_EXCEPTIONS as e:
        raise_for_error(e)
    except Exception as e:
        raise Exception(f"Failed to open streaming handle for {blob_path}") from e


@backoff.on_exception(
    backoff.expo,
    AzureRateLimitError,
    max_tries=6,
    max_time=60,
    jitter=None,
)
@backoff.on_exception(
    backoff.expo,
    AzureBackoffError,
    max_tries=MAX_TRIES,
    factor=2,
)
def get_file_bytes(config, blob_path):
    """
    Download the entire file as bytes in a single HTTP GET (fs.cat).
    ~20x faster than streaming (fs.open) for CSV/text files because it
    issues one bulk request instead of many small range requests.
    Returns a BytesIO object ready for reading.
    """
    try:
        container_name = config['container_name']
        fs_client = setup_azure_client(config)
        raw = fs_client.cat(f'{container_name}/{blob_path}')
        return io.BytesIO(raw)
    except RAW_EXCEPTIONS as e:
        raise_for_error(e)
    except Exception as e:
        raise Exception(f"Failed to bulk-download {blob_path}") from e


def _iter_matching_blobs(config, table_spec):
    """Yield blobs matching table_spec search_prefix and search_pattern."""
    search_prefix = table_spec.get('search_prefix', '') or ''
    root = config.get('root_path', '') or ''
    effective_prefix = f"{root}{search_prefix}" if root else search_prefix
    pattern = table_spec.get('search_pattern')
    if pattern:
        try:
            regex = re.compile(pattern)
        except re.error as e:
            raise ValueError(
                f"Invalid search_pattern {pattern!r} for table "
                f"'{table_spec.get('table_name')}': {e}."
            ) from e
    else:
        regex = None

    for blob in list_files_in_container({**config, 'root_path': effective_prefix}):
        name = blob.name
        if regex is None or regex.search(name):
            yield blob

def get_input_files_for_table(config, table_spec, modified_since=None):
    """
    Get all files matching the table spec pattern and modified since the given timestamp.
    Yields dictionaries with 'key' and 'last_modified' for each matching file.
    """
    pattern = table_spec.get('search_pattern', '')

    matched_files_count = 0

    for blob in _iter_matching_blobs(config, table_spec):
        last_modified = getattr(blob, 'last_modified', None)
        if not last_modified:
            LOGGER.warning("Skipping blob \"%s\" - no last_modified timestamp", blob.name)
            continue

        if modified_since is None or last_modified >= modified_since:
            matched_files_count += 1
            yield {'key': blob.name, 'last_modified': last_modified}

    if matched_files_count == 0:
        LOGGER.warning("No files found matching pattern \"%s\" modified since %s",
                      pattern, modified_since)

def _get_records_for_csv(blob_path, sample_rate, buffer, table_spec, max_records=DEFAULT_MAX_RECORDS):
    current_row = 0
    sampled_row_count = 0
    try:
        buffer.seek(0)
        iterator = singer_csv.get_row_iterator(buffer, table_spec, None, True)
        if not iterator:
            LOGGER.warning("CSV iterator is None for %s", blob_path)
            return
        for row in iterator:
            if len(row) == 0:
                current_row += 1
                continue
            if (current_row % sample_rate) == 0:
                if row.get(SDC_EXTRA_COLUMN):
                    row.pop(SDC_EXTRA_COLUMN)
                sampled_row_count += 1
                yield row
                if max_records is not None and sampled_row_count >= max_records:
                    break
            current_row += 1
    except Exception as e:
        raise Exception(f"Error sampling CSV file {blob_path}") from e

def _get_records_for_jsonl(sample_rate, data_bytes, max_records=DEFAULT_MAX_RECORDS):
    current_row = 0
    sampled_count = 0
    for row in singer_jsonl.get_row_iterator(io.BytesIO(data_bytes)):
        if (current_row % sample_rate) == 0 and isinstance(row, dict):
            yield row
            sampled_count += 1
            if max_records is not None and sampled_count >= max_records:
                break
        current_row += 1

def _get_records_for_json(sample_rate, data_bytes, max_records=DEFAULT_MAX_RECORDS):
    try:
        loaded = json.loads(data_bytes.decode('utf-8'))
    except Exception as e:
        raise Exception("Failed to parse JSON data") from e
    if isinstance(loaded, list):
        sampled_count = 0
        for idx, item in enumerate(loaded):
            if (idx % sample_rate) == 0 and isinstance(item, dict):
                yield item
                sampled_count += 1
                if max_records is not None and sampled_count >= max_records:
                    break
    elif isinstance(loaded, dict):
        # Single JSON object; yield as one sample
        yield loaded

def _get_records_for_parquet(sample_rate, data_bytes, max_records=DEFAULT_MAX_RECORDS):
    row_idx = 0
    sampled_count = 0
    for row in singer_parquet.get_row_iterator(io.BytesIO(data_bytes)):
        if (row_idx % sample_rate) == 0 and isinstance(row, dict):
            yield row
            sampled_count += 1
            if max_records is not None and sampled_count >= max_records:
                break
        row_idx += 1

def _get_records_for_avro(sample_rate, data_bytes, max_records=DEFAULT_MAX_RECORDS):
    sampled_count = 0
    for idx, record in enumerate(singer_avro.get_row_iterator(io.BytesIO(data_bytes))):
        if (idx % sample_rate) == 0 and isinstance(record, dict):
            yield record
            sampled_count += 1
            if max_records is not None and sampled_count >= max_records:
                break

def sample_file(table_spec, blob_path, data, sample_rate, extension, max_records=DEFAULT_MAX_RECORDS):
    """
    Sample records from a single file based on its extension.

    Args:
        table_spec: Table specification dictionary
        blob_path: Path to the Azure blob file
        data: File data as bytes
        sample_rate: Sample every Nth record
        extension: File extension
        max_records: Maximum number of records to sample from this file

    Returns:
        Generator of sampled records
    """
    global skipped_files_count

    # Delimited text: CSV/TXT/TSV/PSV
    if extension in ['csv', 'txt', 'tsv', 'psv']:
        ts = dict(table_spec or {})
        if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
            if extension == 'tsv':
                ts['delimiter'] = '\t'
            elif extension == 'psv':
                ts['delimiter'] = '|'
            else:
                ts['delimiter'] = ','
        # Directly yield from the CSV iterator instead of returning
        yield from _get_records_for_csv(blob_path, sample_rate, io.BytesIO(data), ts, max_records)
        return
    elif extension == 'jsonl':
        yield from _get_records_for_jsonl(sample_rate, data, max_records)
    elif extension == 'json':
        yield from _get_records_for_json(sample_rate, data, max_records)
    elif extension == 'parquet':
        yield from _get_records_for_parquet(sample_rate, data, max_records)
    elif extension == 'avro':
        yield from _get_records_for_avro(sample_rate, data, max_records)
    elif extension in ['xlsx']:
        # Excel files will be handled by singer_encodings.excel_reader
        try:
            from singer_encodings import excel_reader
            options = {
                'key_properties': table_spec.get('key_properties', []),
                'date_overrides': table_spec.get('date_overrides', [])
            }
            iterator = excel_reader.get_excel_row_iterator(
                io.BytesIO(data),
                options=options
            )
            if iterator is None:
                # Empty Excel file
                LOGGER.warning("Skipping \"%s\" file as it is empty", blob_path)
                skipped_files_count += 1
                return

            idx = 0
            sampled_count = 0
            for _, row_dict in iterator:
                if (idx % sample_rate) == 0 and isinstance(row_dict, dict):
                    yield unwrap_excel_commented_cells(row_dict)
                    sampled_count += 1
                    if max_records is not None and sampled_count >= max_records:
                        break
                idx += 1
        except Exception as e:
            raise Exception(f"Failed to sample Excel file {blob_path}") from e
    else:
        LOGGER.warning("\"%s\" with unsupported extension \".%s\" will not be sampled.", blob_path, extension)
        skipped_files_count += 1
        return []

def sampling_gz_file(table_spec, blob_path, data, sample_rate, max_records=DEFAULT_MAX_RECORDS):
    """
    Handle sampling of .gz compressed files.

    Args:
        table_spec: Table specification dictionary
        blob_path: Path to the Azure blob file
        data: Compressed file data as bytes
        sample_rate: Sample every Nth record
        max_records: Maximum number of records to sample

    Returns:
        Generator of sampled records or empty list
    """
    global skipped_files_count

    if blob_path.endswith('.tar.gz'):
        LOGGER.warning("Skipping \"%s\" file as .tar.gz extension is not supported", blob_path)
        skipped_files_count += 1
        return []

    try:
        gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(data))
        gz_data = gz_file_obj.read()

        # Get the original filename from gzip header
        try:
            gz_file_name = get_file_name_from_gzfile(fileobj=io.BytesIO(data))
        except (AttributeError, OSError):
            LOGGER.warning("Skipping \"%s\" - could not get original file name from gzip header", blob_path)
            skipped_files_count += 1
            return []

        if not gz_file_name:
            LOGGER.warning("Skipping \"%s\" - no filename found in gzip header", blob_path)
            skipped_files_count += 1
            return []

        gz_lower = gz_file_name.lower()

        # Check for nested compression
        if gz_lower.endswith('.gz'):
            LOGGER.warning("Skipping \"%s\" - nested compression not supported", blob_path)
            skipped_files_count += 1
            return []

        gz_extension = gz_lower.split('.')[-1]
        full_path = f"{blob_path}/{gz_file_name}"
        return sample_file(table_spec, full_path, gz_data, sample_rate, gz_extension, max_records)

    except Exception as e:
        raise Exception(f"Failed to process GZ file {blob_path}") from e

def sampling_zip_file(table_spec, blob_path, data, sample_rate, max_records=DEFAULT_MAX_RECORDS):
    """
    Handle sampling of .zip compressed files.

    Args:
        table_spec: Table specification dictionary
        blob_path: Path to the Azure blob file
        data: Compressed file data as bytes
        sample_rate: Sample every Nth record
        max_records: Maximum number of records to sample

    Yields:
        dict: Sampled records from files in the zip
    """
    global skipped_files_count

    try:
        decompressed_files = compression.infer(io.BytesIO(data), blob_path)

        for decompressed_file in decompressed_files:
            de_name = decompressed_file.name
            de_lower = de_name.lower()
            de_extension = de_lower.split('.')[-1]

            # Skip nested compressed files
            if de_extension in ['zip', 'gz', 'tar']:
                LOGGER.warning("Skipping \"%s/%s\" - nested compression not supported for sampling", blob_path, de_name)
                skipped_files_count += 1
                continue

            # Sample the extracted file
            full_path = f"{blob_path}/{de_name}"
            de_data = decompressed_file.read()
            # Yield from each file's samples
            for record in sample_file(table_spec, full_path, de_data, sample_rate, de_extension, max_records):
                yield record

    except Exception as e:
        raise Exception(f"Failed to process ZIP file {blob_path}") from e

@backoff.on_exception(
    backoff.expo,
    AzureRateLimitError,
    max_tries=6,
    max_time=60,
    jitter=None,
)
@backoff.on_exception(
    backoff.expo,
    AzureBackoffError,
    max_tries=MAX_TRIES,
    factor=2,
)
def _download_blob_with_retry(fs_client, container_name, file_key):
    """Download blob contents with automatic retry on transient errors."""
    try:
        with fs_client.open(f'{container_name}/{file_key}', 'rb') as f:
            return f.read()
    except RAW_EXCEPTIONS as e:
        raise_for_error(e)


def get_files_to_sample(config, azure_files, max_files):
    """
    Prepare Azure blob files for sampling, downloading and extracting compressed files.

    Args:
        config: Configuration dictionary
        azure_files: List of Azure blob file metadata
        max_files: Maximum number of files to sample

    Returns:
        list: List of file dictionaries ready for sampling
    """
    global skipped_files_count
    sampled_files = []
    container_name = config.get('container_name')

    try:
        fs_client = setup_azure_client(config)
    except Exception as e:
        raise Exception("Failed to setup Azure client") from e

    for azure_file in azure_files:
        if len(sampled_files) >= max_files:
            break

        file_key = azure_file.get('key')
        if not file_key:
            continue

        try:
            data = _download_blob_with_retry(fs_client, container_name, file_key)
        except (AzureBackoffError, AzureRateLimitError):
            # Retries are already exhausted by the backoff decorator; let the error propagate.
            raise
        except Exception as e:
            raise Exception(f"Failed to download file {file_key}") from e

        file_name = file_key.split("/")[-1]
        lower_name = file_name.lower()

        # Check if file is without extension
        if '.' not in file_name or lower_name == file_name.split('.')[-1]:
            LOGGER.warning("\"%s\" without extension will not be sampled.", file_key)
            skipped_files_count += 1
            continue

        extension = lower_name.split('.')[-1]

        # Check if file is gzipped even with non-gz extension (magic bytes: 1f 8b)
        is_gzipped = len(data) >= 2 and data[0] == 0x1f and data[1] == 0x8b
        if is_gzipped and extension != 'gz':
            try:
                original_name = get_file_name_from_gzfile(fileobj=io.BytesIO(data))
                if original_name:
                    extension = original_name.lower().split('.')[-1]
                gz_file_obj = gzip.GzipFile(fileobj=io.BytesIO(data))
                data = gz_file_obj.read()
            except Exception as e:
                raise Exception(f"Failed to decompress gzipped file {file_key}") from e

        sampled_files.append({
            'blob_path': file_key,
            'data': data,
            'extension': extension
        })

    return sampled_files

def sample_files(
    config,
    table_spec,
    azure_files,
    sample_rate=DEFAULT_SAMPLE_RATE,
    max_records=DEFAULT_MAX_RECORDS,
    max_files=DEFAULT_MAX_FILES):
    """
    Sample records from multiple Azure blob files.

    Args:
        config: Configuration dictionary
        table_spec: Table specification dictionary
        azure_files: List of Azure blob file metadata
        sample_rate: Sample every Nth record
        max_records: Maximum total records to sample
        max_files: Maximum number of files to sample

    Yields:
        dict: Sampled records
    """
    global skipped_files_count

    for azure_file in itertools.islice(get_files_to_sample(config, azure_files, max_files), max_files):
        blob_path = azure_file.get('blob_path', '')
        data = azure_file.get('data')
        extension = azure_file.get('extension')

        try:
            sample_count = 0
            if extension == 'gz':
                for record in itertools.islice(sampling_gz_file(table_spec, blob_path, data, sample_rate, max_records), max_records):
                    sample_count += 1
                    yield record
            elif extension == 'zip':
                for record in itertools.islice(sampling_zip_file(table_spec, blob_path, data, sample_rate, max_records), max_records):
                    sample_count += 1
                    yield record
            else:
                for record in sample_file(table_spec, blob_path, data, sample_rate, extension, max_records):
                    sample_count += 1
                    yield record
        except Exception as e:
            raise Exception(f"Failed to sample file {blob_path}") from e

def get_sampled_schema_for_table(config, table_spec):
    global skipped_files_count
    skipped_files_count = 0

    azure_files_gen = get_input_files_for_table(config, table_spec)
    azure_files_list = list(azure_files_gen)  # Convert to list to check if empty

    if not azure_files_list:
        # No files matched the spec at all
        LOGGER.info("No files found for table \"%s\"", table_spec.get('table_name'))
        return None

    samples = [sample for sample in sample_files(config, table_spec, iter(azure_files_list))]

    if skipped_files_count:
        LOGGER.warning("%s files got skipped during the last sampling.", skipped_files_count)

    if not samples:
        # Files were found but sampling failed or no data in files
        LOGGER.warning("Files found but no samples could be extracted for table '%s'", table_spec.get('table_name'))
        return {
            'type': 'object',
            'properties': {}
        }

    metadata_schema = {
        SDC_SOURCE_CONTAINER_COLUMN: {'type': 'string'},
        SDC_SOURCE_FILE_COLUMN: {'type': 'string'},
        SDC_SOURCE_LINENO_COLUMN: {'type': 'integer'},
        SDC_EXTRA_COLUMN: {
            'type': ['null', 'array'],
            'items': {'type': 'object', 'properties': {}}
        }
    }

    data_schema = conversion.generate_schema(samples, table_spec)

    return {
        'type': 'object',
        'properties': {**data_schema, **metadata_schema}
    }
