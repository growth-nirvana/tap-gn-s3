# tap-gn-s3

`tap-gn-s3` is a Singer tap for extracting data from CSV files stored in Amazon S3 buckets. It supports both regular CSV files and ZIP archives containing CSV files.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- Extracts data from CSV files in S3 buckets
- Supports ZIP archives containing multiple CSV files
- Automatic schema detection from CSV headers
- BigQuery-compliant column name formatting
- Incremental state tracking using file modification timestamps
- Configurable CSV parsing options (delimiter, encoding, etc.)
- Support for date/time field type overrides

## Installation

```bash
pipx install tap-gn-s3
```

## Configuration

### Required Configuration Settings

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `bucket` | string | Yes | S3 bucket name containing the CSV files |
| `start_date` | datetime | Yes | The earliest record date to sync (ISO format) |
| `tables` | array | Yes | Array of table configurations (see Table Configuration below) |

### AWS Authentication Settings

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `aws_access_key_id` | string | No* | AWS access key ID |
| `aws_secret_access_key` | string | No* | AWS secret access key |
| `aws_session_token` | string | No | AWS session token |
| `aws_profile` | string | No | AWS profile name |
| `aws_endpoint_url` | string | No | AWS endpoint URL (for non-AWS S3) |

*Either `aws_access_key_id`/`aws_secret_access_key` pair or `aws_profile` must be provided.

### Optional Settings

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `table_suffix` | string | No | Suffix to append to table names |
| `warning_if_no_files` | boolean | No | Issue warning instead of error if no files found |
| `s3_proxies` | object | No | Proxy settings for S3 (`http` and `https` URLs) |

### Table Configuration

Each entry in the `tables` array supports the following settings:

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `table_name` | string | Yes | Name of the target table |
| `search_pattern` | string | Yes | Regex pattern to match file names |
| `search_prefix` | string | No | S3 prefix to limit file search |
| `key_properties` | array | No | List of primary key field names |
| `date_overrides` | array | No | List of fields to treat as dates |
| `string_overrides` | array | No | List of fields to treat as strings |
| `datatype_overrides` | object | No | Custom data type mappings |
| `guess_types` | boolean | No | Whether to attempt type inference |
| `delimiter` | string | No | CSV delimiter character (default: ",") |
| `remove_character` | string | No | Character to remove from values |
| `encoding` | string | No | File encoding (default: "utf-8") |
| `set_empty_values_null` | boolean | No | Convert empty values to null |

### Example Config

```json
{
  "bucket": "my-data-bucket",
  "aws_access_key_id": "YOUR_ACCESS_KEY",
  "aws_secret_access_key": "YOUR_SECRET_KEY",
  "start_date": "2024-01-01T00:00:00Z",
  "tables": [
    {
      "table_name": "sales_data",
      "search_pattern": "sales_.*\\.csv",
      "search_prefix": "data/sales/",
      "date_overrides": ["transaction_date", "updated_at"],
      "delimiter": ",",
      "encoding": "utf-8"
    }
  ]
}
```

## Usage

You can easily run `tap-gn-s3` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-gn-s3 --version
tap-gn-s3 --help
tap-gn-s3 --config CONFIG --discover > ./catalog.json
```

### Using with Meltano

```bash
# Install meltano
pipx install meltano

# Initialize meltano within this directory
cd tap-gn-s3
meltano install

# Test invocation:
meltano invoke tap-gn-s3 --version

# Run a test ELT pipeline:
meltano run tap-gn-s3 target-jsonl
```

## Developer Resources

### Initialize your Development Environment

Prerequisites:
- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

```bash
uv run pytest
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to develop your own taps and targets.
