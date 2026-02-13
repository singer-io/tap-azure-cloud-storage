# tap-azure-cloud-storage

A [Singer](https://singer.io) tap for extracting data from Azure Blob Storage (Azure Cloud Storage).

## Overview

`tap-azure-cloud-storage` is a Singer tap that extracts data from Azure Blob Storage containers. It supports multiple file formats including CSV, JSON-L, Parquet, Avro, and Excel files, and can handle both full table and incremental replication.

## Features

### Supported File Formats
- **Delimited text files**: CSV, TSV, PSV (pipe-separated), and custom delimiters
- **JSON**: JSON-L (newline-delimited JSON)
- **Parquet**: Apache Parquet columnar format
- **Avro**: Apache Avro format
- **Excel**: .xlsx files
- **Compressed files**: Gzip (.gz) and Zip (.zip) archives

### Authentication Methods
- **Service Principal**: Using Azure AD application credentials (recommended for production)
- **Connection String**: Direct connection string authentication
- **Account Key**: Storage account key authentication
- **Managed Identity**: DefaultAzureCredential for Azure-hosted environments

### Key Capabilities
- **Flexible file selection**: Specify folder paths and use regex patterns to match files
- **Multiple tables**: Define multiple table configurations within a single connection
- **Incremental replication**: Support for incremental sync based on file modification time
- **Primary key support**: Define primary keys for incremental replication
- **DateTime field detection**: Automatic detection and handling of datetime fields
- **Schema inference**: Automatic schema detection from file contents
- **UTF-8 encoding**: Full UTF-8 support
- **Compression**: Automatic handling of Gzip compressed files

## Installation

### Prerequisites
- Python 3.7 or higher
- pip

### Install from source

```bash
git clone <repository-url>
cd tap-azure-cloud-storage
pip install -e .
```

## Configuration

### Required Configuration

Create a `config.json` file with the following structure:

```json
{
  "storage_account_name": "your_storage_account",
  "container_name": "your_container_name",
  "start_date": "2024-01-01T00:00:00Z",
  "tables": [
    {
      "table_name": "my_table",
      "search_pattern": ".*\\.csv$",
      "search_prefix": "path/to/files/",
      "key_properties": ["id"],
      "date_overrides": ["created_at", "updated_at"],
      "delimiter": ","
    }
  ]
}
```

### Authentication Configuration

#### Method 1: Service Principal (Recommended for Production)

Add these fields to your `config.json`:

```json
{
  "storage_account_name": "your_storage_account",
  "tenant_id": "your-tenant-id",
  "client_id": "your-client-id",
  "client_secret": "your-client-secret",
  ...
}
```

**Setting up Service Principal:**

1. Create an Azure AD Application:
   ```bash
   az ad app create --display-name "tap-azure-storage-app"
   ```

2. Create a Service Principal:
   ```bash
   az ad sp create --id <application-id>
   ```

3. Assign Storage Blob Data Reader role:
   ```bash
   az role assignment create \
     --role "Storage Blob Data Reader" \
     --assignee <service-principal-id> \
     --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>"
   ```

4. Create a client secret:
   ```bash
   az ad app credential reset --id <application-id>
   ```

#### Method 2: Connection String

```json
{
  "storage_account_name": "your_storage_account",
  "connection_string": "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net",
  ...
}
```

#### Method 3: Account Key

```json
{
  "storage_account_name": "your_storage_account",
  "account_key": "your-account-key",
  ...
}
```

#### Method 4: Managed Identity

For Azure-hosted environments (Azure VMs, Azure Functions, etc.):

```json
{
  "storage_account_name": "your_storage_account",
  ...
}
```

### Configuration Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `storage_account_name` | Yes | Azure Storage account name |
| `container_name` | Yes | Azure Blob Storage container name |
| `start_date` | Yes | Start date for incremental sync (ISO 8601 format) |
| `tables` | Yes | Array of table configurations (see below) |
| `tenant_id` | No | Azure AD tenant ID (for Service Principal auth) |
| `client_id` | No | Azure AD application client ID (for Service Principal auth) |
| `client_secret` | No | Azure AD application client secret (for Service Principal auth) |
| `connection_string` | No | Azure Storage connection string |
| `account_key` | No | Azure Storage account key |
| `root_path` | No | Root path prefix within the container |

### Table Configuration

Each table in the `tables` array supports the following parameters:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `table_name` | Yes | Name of the table/stream |
| `search_pattern` | Yes | Regex pattern to match file names (e.g., `".*\\.csv$"`) |
| `search_prefix` | No | Folder path prefix to narrow file search |
| `key_properties` | Yes | Array of field names to use as primary keys (use `[]` for full table sync) |
| `date_overrides` | No | Array of field names to treat as datetime fields |
| `delimiter` | No | Custom delimiter for CSV files (auto-detected for .tsv, .psv) |

### Example Configurations

#### Multiple Tables with Different Formats

```json
{
  "storage_account_name": "myaccount",
  "container_name": "data",
  "tenant_id": "...",
  "client_id": "...",
  "client_secret": "...",
  "start_date": "2024-01-01T00:00:00Z",
  "tables": [
    {
      "table_name": "customers",
      "search_pattern": "customers.*\\.csv$",
      "search_prefix": "exports/customers/",
      "key_properties": ["customer_id"],
      "date_overrides": ["created_at"],
      "delimiter": ","
    },
    {
      "table_name": "orders",
      "search_pattern": "orders.*\\.parquet$",
      "search_prefix": "exports/orders/",
      "key_properties": ["order_id"],
      "date_overrides": ["order_date"]
    },
    {
      "table_name": "products",
      "search_pattern": "products.*\\.jsonl$",
      "search_prefix": "exports/products/",
      "key_properties": [],
      "date_overrides": []
    }
  ]
}
```

#### TSV Files with Custom Settings

```json
{
  "storage_account_name": "myaccount",
  "container_name": "data",
  "account_key": "...",
  "start_date": "2024-01-01T00:00:00Z",
  "tables": [
    {
      "table_name": "events",
      "search_pattern": "events_.*\\.tsv$",
      "search_prefix": "logs/",
      "key_properties": ["event_id"],
      "date_overrides": ["event_timestamp"],
      "delimiter": "\t"
    }
  ]
}
```

## Usage

### Discovery Mode

Discover the schema of your data:

```bash
tap-azure-cloud-storage --config config.json --discover > catalog.json
```

### Select Streams

Edit the `catalog.json` to select streams and fields:

```json
{
  "streams": [
    {
      "tap_stream_id": "my_table",
      "stream": "my_table",
      "schema": {...},
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "selected": true,
            "table-key-properties": ["id"]
          }
        },
        ...
      ]
    }
  ]
}
```

### Sync Data

Run the tap to extract data:

```bash
tap-azure-cloud-storage --config config.json --catalog catalog.json
```

### With a Singer Target

Pipe data to a Singer target:

```bash
tap-azure-cloud-storage --config config.json --catalog catalog.json | target-jsonl > output.jsonl
```

### Incremental Sync with State

```bash
tap-azure-cloud-storage --config config.json --catalog catalog.json --state state.json | target-jsonl > output.jsonl
```

## Replication Methods

### Full Table Replication

When `key_properties` is empty (`[]`), the tap performs a full table replication on every run, extracting all matched files.

```json
{
  "table_name": "my_table",
  "search_pattern": ".*\\.csv$",
  "key_properties": []
}
```

### Incremental Replication

When `key_properties` contains field names, the tap performs incremental replication based on file modification time. Only files modified since the last sync are processed.

```json
{
  "table_name": "my_table",
  "search_pattern": ".*\\.csv$",
  "key_properties": ["id"]
}
```

## Metadata Columns

The tap automatically adds metadata columns to each record:

- `_sdc_source_container`: Azure Blob Storage container name
- `_sdc_source_file`: Full path to the source file
- `_sdc_source_lineno`: Line number within the source file
- `_sdc_extra`: Extra fields not defined in the schema (for JSONL files)

## File Format Specifics

### CSV/TSV/PSV Files
- Automatic delimiter detection based on file extension
- Custom delimiter support via `delimiter` parameter
- Header row required
- UTF-8 encoding

### JSON-L Files
- One JSON object per line
- Automatic schema inference
- Supports nested objects and arrays
- Extra fields stored in `_sdc_extra` column

### Parquet Files
- Schema read directly from Parquet metadata
- Efficient columnar reading
- Supports nested structures

### Avro Files
- Schema read from Avro file metadata
- Supports complex types
- Efficient binary format

### Excel Files (.xlsx)
- First row treated as headers
- Single worksheet support
- Automatic type inference

### Compressed Files
- **Gzip (.gz)**: Automatically decompressed
- **Zip (.zip)**: All contained files processed
- Nested compression not supported

## Permissions and Security

### Minimum Required Permissions

For production use with Service Principal authentication, assign the **Storage Blob Data Reader** role:

```bash
az role assignment create \
  --role "Storage Blob Data Reader" \
  --assignee <service-principal-id> \
  --scope "/subscriptions/<subscription-id>/resourceGroups/<resource-group>/providers/Microsoft.Storage/storageAccounts/<storage-account>/blobServices/default/containers/<container>"
```

### Recommended Security Practices

1. **Use Service Principal authentication** in production
2. **Apply least privilege principle** - only grant read access to specific containers
3. **Rotate credentials regularly**
4. **Use Azure Key Vault** to store secrets
5. **Enable Azure Storage logging** for audit trails
6. **Use private endpoints** when possible to avoid public internet exposure

## Troubleshooting

### Connection Issues

**Problem**: "Failed to connect to Azure"

**Solutions**:
- Verify `storage_account_name` is correct
- Check authentication credentials (tenant_id, client_id, client_secret)
- Ensure Service Principal has proper permissions
- Verify network connectivity to Azure

### Authentication Issues

**Problem**: "Authentication failed"

**Solutions**:
- For Service Principal: Verify client_secret is correct and not expired
- Check that the Service Principal has "Storage Blob Data Reader" role
- Ensure tenant_id and client_id are correct

### File Discovery Issues

**Problem**: "No objects matched for table"

**Solutions**:
- Check `search_pattern` regex syntax
- Verify `search_prefix` path exists in container
- Ensure files have correct extensions
- Check file modification dates against `start_date`

### Schema Detection Issues

**Problem**: Schema not detected correctly

**Solutions**:
- Ensure files have consistent structure
- Check file encoding (must be UTF-8)
- For CSV files, verify headers are present
- Increase sampling by adding more files matching the pattern

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Style

```bash
pylint tap_azure_cloud_storage
```

## Support

For issues, questions, or contributions, please contact your Qlik support representative or open an issue in the project repository.

## License

MIT License - See LICENSE file for details

## Changelog

### Version 0.0.1
- Initial release
- Support for CSV, TSV, PSV, JSON-L, Parquet, Avro, and Excel files
- Multiple authentication methods (Service Principal, Connection String, Account Key, Managed Identity)
- Full and incremental replication
- Gzip and Zip compression support
- Regex pattern matching for file selection
- Automatic schema inference
