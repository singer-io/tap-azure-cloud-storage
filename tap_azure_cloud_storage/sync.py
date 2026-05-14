import sys
import csv
import io
import json
import gzip

from singer import metadata
from singer import Transformer
from singer import utils as singer_utils

import singer
from singer_encodings import (
    avro,
    compression,
    csv as csv_helper,
    jsonl,
    parquet
)
from tap_azure_cloud_storage import azure_storage
from tap_azure_cloud_storage.azure_storage import get_file_name_from_gzfile
from tap_azure_cloud_storage.exceptions import AzureBackoffError, AzureRateLimitError


LOGGER = singer.get_logger()


def stream_is_selected(mdata_map):
    return mdata_map.get((), {}).get('selected', False)


def sync_stream(config, state, table_spec, stream, sync_start_time):
    table_name = table_spec['table_name']
    modified_since = singer_utils.strptime_with_tz(
        singer.get_bookmark(state, table_name, 'modified_since') or config['start_date']
    )

    LOGGER.info("Syncing table \"%s\". Getting files modified since %s.", table_name, modified_since)

    # Reset skipped files counter for this stream to get accurate per-stream counts
    azure_storage.skipped_files_count = 0

    azure_files = azure_storage.get_input_files_for_table(config, table_spec, modified_since)

    records_streamed = 0

    for azure_file in sorted(azure_files, key=lambda item: item['last_modified']):
        records_streamed += sync_table_file(config, azure_file['key'], table_spec, stream)
        if azure_file['last_modified'] < sync_start_time:
            state = singer.write_bookmark(state, table_name, 'modified_since', azure_file['last_modified'].isoformat())
        else:
            state = singer.write_bookmark(state, table_name, 'modified_since', sync_start_time.isoformat())
        singer.write_state(state)

    if azure_storage.skipped_files_count:
        LOGGER.warning("%s files got skipped during the last sync.", azure_storage.skipped_files_count)

    return records_streamed


def sync_table_file(config, blob_path, table_spec, stream):

    extension = blob_path.split(".")[-1].lower()

    if not extension or blob_path.lower() == extension:
        LOGGER.warning("\"%s\" without extension will not be synced.", blob_path)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
        return 0
    try:
        if extension == "gz":
            return sync_gz_file(config, blob_path, table_spec, stream)
        if extension == "zip":
            return sync_compressed_file(config, blob_path, table_spec, stream)
        if extension in ["csv", "jsonl", "txt", "tsv", "psv", "parquet", "avro", "xlsx"]:
            return handle_file(config, blob_path, table_spec, stream, extension)
        LOGGER.warning("\"%s\" having the \".%s\" extension will not be synced.", blob_path, extension)
    except (AzureBackoffError, AzureRateLimitError):
        # Let transient server errors propagate so the caller can surface them.
        # These have already been retried by the backoff decorators in azure_storage.py.
        raise
    except (UnicodeDecodeError, json.decoder.JSONDecodeError):
        LOGGER.warning("Skipping %s file as parsing failed. Verify an extension of the file.", blob_path)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
    return 0


def handle_file(config, blob_path, table_spec, stream, extension, file_handler=None):
    # Check if file is gzipped despite non-gz extension (magic bytes: 1f 8b)
    # This handles files like gz_stored_as_csv.csv which are gzipped but have .csv extension
    if extension in ["csv", "txt", "tsv", "psv", "jsonl"] and not file_handler:
        # Bulk-download the file in one HTTP GET (20x faster than streaming for CSV).
        # This also lets us inspect the gzip magic bytes without a separate open().
        file_handle = azure_storage.get_file_bytes(config, blob_path)
        if file_handle:
            peek_data = file_handle.read(2)
            file_handle.seek(0)  # rewind — BytesIO supports seeking at no cost

            if len(peek_data) >= 2 and peek_data[0] == 0x1f and peek_data[1] == 0x8b:
                # Treat as a gz file instead
                return sync_gz_file(config, blob_path, table_spec, stream, file_handler=file_handle)

    # Track if we own the file handle (need to close it)
    # If file_handler was passed in, caller owns it; otherwise we need to manage it
    own_handle = file_handler is None
    file_handle = file_handler

    try:
        if extension in ["csv", "txt", "tsv", "psv"]:
            # Use bulk-download (fs.cat) for CSV/text — ~20x faster than streaming.
            # Already downloaded above for non-handler paths; fall back for handler paths.
            if file_handle is None:
                file_handle = azure_storage.get_file_bytes(config, blob_path)
            if file_handle is None:
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                return 0
            return sync_csv_file(config, file_handle, blob_path, table_spec, stream)

        if extension == "parquet":
            if file_handle is None:
                file_handle = azure_storage.get_file_bytes(config, blob_path)
            if file_handle is None:
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                return 0
            return sync_parquet_file(config, file_handle, blob_path, table_spec, stream)

        if extension == "avro":
            if file_handle is None:
                file_handle = azure_storage.get_file_bytes(config, blob_path)
            if file_handle is None:
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                return 0
            return sync_avro_file(config, file_handle, blob_path, table_spec, stream)

        if extension == "jsonl":
            if file_handle is None:
                file_handle = azure_storage.get_file_bytes(config, blob_path)
            if file_handle is None:
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                return 0
            iterator = jsonl.get_row_iterator(file_handle)
            records = sync_jsonl_file(config, iterator, blob_path, table_spec, stream)
            if records == 0:
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                LOGGER.warning("Skipping \"%s\" file as it is empty", blob_path)
            return records

        if extension in ["xlsx"]:
            if file_handle is None:
                file_handle = azure_storage.get_file_bytes(config, blob_path)
            if file_handle is None:
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                return 0
            return sync_excel_file(config, file_handle, blob_path, table_spec, stream)

        if extension in ("zip", "gz") and file_handler:
            # Inside a compressed archive – skip nested compression to prevent infinite loops
            LOGGER.warning("Skipping \"%s\" file as it contains nested compression.", blob_path)
            azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
            return 0

        LOGGER.warning("\"%s\" having the \".%s\" extension will not be synced.", blob_path, extension)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
        return 0
    finally:
        # Close file handle only if we opened it (not passed from caller)
        if own_handle and file_handle is not None:
            try:
                file_handle.close()
            except Exception as e:
                LOGGER.warning("Failed to close file handle for \"%s\": %s", blob_path, e)


def sync_gz_file(config, blob_path, table_spec, stream, file_handler=None):
    """Handle .gz files by reading the original filename from gzip header."""
    # If file is extracted from zip use file object else get file object from Azure blob storage
    file_object = file_handler if file_handler else azure_storage.get_file_handle(config, blob_path)
    if file_object is None:
        return 0

    # Track whether this function owns the file handle so we can close it.
    own_handle = file_handler is None

    try:
        # Ensure we read the gzip header from the beginning of the file, if possible.
        if hasattr(file_object, "seek"):
            file_object.seek(0)

        try:
            gz_file_name = get_file_name_from_gzfile(fileobj=file_object)
        except (AttributeError, OSError) as err:
            # If a file is compressed using gzip command with --no-name attribute,
            # It will not return the file name and timestamp. Hence we will skip such files.
            LOGGER.warning("Skipping \"%s\" file as we did not get the original file name", blob_path)
            azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
            return 0

        if gz_file_name:
            if gz_file_name.endswith(".gz"):
                LOGGER.warning("Skipping \"%s\" file as it contains nested compression.", blob_path)
                azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
                return 0

            # Prepare to stream decompressed data from the gzip file.
            gz_file_extension = gz_file_name.split(".")[-1].lower()

            # Reset to the beginning again before constructing the GzipFile.
            if hasattr(file_object, "seek"):
                file_object.seek(0)

            gz_file_obj = gzip.GzipFile(fileobj=file_object)
            try:
                return handle_file(
                    config,
                    blob_path + "/" + gz_file_name,
                    table_spec,
                    stream,
                    gz_file_extension,
                    gz_file_obj,
                )
            finally:
                # Ensure the gzip wrapper is closed after use.
                gz_file_obj.close()

        LOGGER.warning("Skipping \"%s\" file - no filename found in gzip header", blob_path)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
        return 0
    finally:
        # Close the underlying file handle if we opened it in this function.
        if own_handle and file_object is not None:
            try:
                file_object.close()
            except Exception:
                # Swallow any exception during close to avoid masking the original error.
                LOGGER.warning("Failed to close file handle for \"%s\"", blob_path)
def sync_compressed_file(config, blob_path, table_spec, stream):
    """Handle .zip files by extracting and syncing contents."""
    LOGGER.info("Syncing Compressed file \"%s\".", blob_path)

    records_streamed = 0
    # Use adlfs for streaming compressed files
    file_handle = azure_storage.get_file_handle(config, blob_path)
    if file_handle is None:
        return 0

    try:
        # Read the file content for decompression
        # Note: compression.infer needs the data, but adlfs streams it efficiently
        decompressed_files = compression.infer(io.BytesIO(file_handle.read()), blob_path)

        for decompressed_file in decompressed_files:
            extension = decompressed_file.name.split(".")[-1].lower()

            if extension in ["csv", "jsonl", "gz", "txt", "tsv", "psv", "xlsx"]:
                blob_file_path = blob_path + "/" + decompressed_file.name
                records_streamed += handle_file(
                    config,
                    blob_file_path,
                    table_spec,
                    stream,
                    extension,
                    file_handler=decompressed_file,
                )
    finally:
        file_handle.close()

    return records_streamed


def sync_csv_file(config, file_handle, blob_path, table_spec, stream):
    LOGGER.info("Syncing file \"%s\".", blob_path)

    container = config['container_name']
    table_name = table_spec['table_name']

    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        # On Windows, C long may be 32-bit; fall back to max 32-bit int
        csv.field_size_limit(2147483647)

    ts = dict(table_spec)
    lower = blob_path.lower()
    if 'delimiter' not in ts or ts.get('delimiter') in (None, ''):
        if lower.endswith('.tsv'):
            ts['delimiter'] = '\t'
        elif lower.endswith('.psv'):
            ts['delimiter'] = '|'
        else:
            ts['delimiter'] = ','

    if "properties" in stream["schema"]:
        iterator = csv_helper.get_row_iterator(
            file_handle, ts, stream["schema"]["properties"].keys(), True)
    else:
        iterator = csv_helper.get_row_iterator(file_handle, ts, None, True)

    records_synced = 0

    if iterator:
        for row in iterator:
            if len(row) == 0:
                continue

            custom_columns = {
                azure_storage.SDC_SOURCE_CONTAINER_COLUMN: container,
                azure_storage.SDC_SOURCE_FILE_COLUMN: blob_path,
                azure_storage.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
            }
            rec = {**row, **custom_columns}

            with Transformer() as transformer:
                to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

            singer.write_record(table_name, to_write)
            records_synced += 1
    else:
        LOGGER.warning("Skipping \"%s\" file as it is empty", blob_path)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1

    return records_synced


def sync_avro_parquet_file(config, iterator, blob_path, table_spec, stream):
    LOGGER.info("Syncing file \"%s\".", blob_path)

    container = config['container_name']
    table_name = table_spec['table_name']

    records_synced = 0

    if iterator is not None:
        for row in iterator:

            custom_columns = {
                azure_storage.SDC_SOURCE_CONTAINER_COLUMN: container,
                azure_storage.SDC_SOURCE_FILE_COLUMN: blob_path,
                azure_storage.SDC_SOURCE_LINENO_COLUMN: records_synced + 1
            }
            rec = {**row, **custom_columns}

            with Transformer() as transformer:
                to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

            singer.write_record(table_name, to_write)
            records_synced += 1
    else:
        LOGGER.warning("Skipping \"%s\" file as it is empty", blob_path)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1

    return records_synced


def sync_avro_file(config, file_handle, blob_path, table_spec, stream):
    iterator = avro.get_row_iterator(file_handle)
    return sync_avro_parquet_file(config, iterator, blob_path, table_spec, stream)


def sync_parquet_file(config, file_handle, blob_path, table_spec, stream):
    iterator = parquet.get_row_iterator(file_handle)
    return sync_avro_parquet_file(config, iterator, blob_path, table_spec, stream)


def sync_jsonl_file(config, iterator, blob_path, table_spec, stream):
    LOGGER.info("Syncing file \"%s\".", blob_path)

    container = config['container_name']
    table_name = table_spec['table_name']

    records_synced = 0

    for row in iterator:

        custom_columns = {
            azure_storage.SDC_SOURCE_CONTAINER_COLUMN: container,
            azure_storage.SDC_SOURCE_FILE_COLUMN: blob_path,
            azure_storage.SDC_SOURCE_LINENO_COLUMN: records_synced + 1
        }
        rec = {**row, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        value = [{field: rec[field]} for field in set(rec) - set(to_write)]

        if value:
            # Log only field names, not actual data values
            extra_fields = list(set(rec) - set(to_write))
            LOGGER.warning("File '%s': Fields %s not found in catalog and will be stored in \"_sdc_extra\" field.", blob_path, extra_fields)
            extra_data = {azure_storage.SDC_EXTRA_COLUMN: value}
            update_to_write = {**to_write, **extra_data}
        else:
            update_to_write = to_write

        with Transformer() as transformer:
            update_to_write = transformer.transform(update_to_write, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, update_to_write)
        records_synced += 1

    return records_synced


def sync_excel_file(config, file_handle, blob_path, table_spec, stream):
    """Sync Excel (.xlsx) files."""
    LOGGER.info("Syncing Excel file \"%s\".", blob_path)

    container = config['container_name']
    table_name = table_spec['table_name']

    try:
        from singer_encodings import excel_reader
        options = {
            'key_properties': table_spec.get('key_properties', []),
            'date_overrides': table_spec.get('date_overrides', [])
        }
        iterator = excel_reader.get_excel_row_iterator(file_handle, options=options)
    except Exception as e:
        LOGGER.warning("Failed to read Excel file %s: %s", blob_path, e)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
        return 0

    if iterator is None:
        LOGGER.warning("Skipping \"%s\" file as it is empty", blob_path)
        azure_storage.skipped_files_count = azure_storage.skipped_files_count + 1
        return 0

    records_synced = 0
    for sheet_name, row_dict in iterator:
        if not isinstance(row_dict, dict) or len(row_dict) == 0:
            continue

        # Unwrap singer-encodings commented cell wrappers so that
        # the Transformer receives plain values instead of list-of-dict.
        row_dict = azure_storage.unwrap_excel_commented_cells(row_dict)

        custom_columns = {
            azure_storage.SDC_SOURCE_CONTAINER_COLUMN: container,
            azure_storage.SDC_SOURCE_FILE_COLUMN: f"{blob_path}/{sheet_name}",
            azure_storage.SDC_SOURCE_LINENO_COLUMN: records_synced + 2
        }
        rec = {**row_dict, **custom_columns}

        with Transformer() as transformer:
            to_write = transformer.transform(rec, stream['schema'], metadata.to_map(stream['metadata']))

        singer.write_record(table_name, to_write)
        records_synced += 1

    return records_synced
