"""gn-s3 tap class."""

from __future__ import annotations

import logging.config
import os
import json

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_gn_s3.streams import CSVFileStream

# Default logging config
DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(levelname)s %(asctime)s %(name)s: %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default"
        }
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO"
    }
}

class TapGnS3(Tap):
    """gn-s3 tap class."""

    name = "tap-gn-s3"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "aws_access_key_id",
            th.StringType,
            description="AWS access key ID",
            secret=True,
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            description="AWS secret access key",
            secret=True,
        ),
        th.Property(
            "aws_session_token",
            th.StringType,
            description="AWS session token",
            secret=True,
        ),
        th.Property(
            "aws_profile",
            th.StringType,
            description="AWS profile name",
        ),
        th.Property(
            "aws_endpoint_url",
            th.StringType,
            description="AWS endpoint URL (for non-AWS S3)",
        ),
        th.Property(
            "bucket",
            th.StringType,
            required=True,
            description="S3 bucket name",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
        th.Property(
            "tables",
            th.ArrayType(
                th.ObjectType(
                    th.Property("table_name", th.StringType, required=True),
                    th.Property("search_pattern", th.StringType, required=True),
                    th.Property("key_properties", th.ArrayType(th.StringType)),
                    th.Property("search_prefix", th.StringType),
                    th.Property("date_overrides", th.ArrayType(th.StringType)),
                    th.Property("string_overrides", th.ArrayType(th.StringType)),
                    th.Property("datatype_overrides", th.ObjectType()),
                    th.Property("guess_types", th.BooleanType),
                    th.Property("delimiter", th.StringType),
                    th.Property("remove_character", th.StringType),
                    th.Property("encoding", th.StringType),
                    th.Property("set_empty_values_null", th.BooleanType),
                )
            ),
            required=True,
            description="Table configurations",
        ),
        th.Property(
            "table_suffix",
            th.StringType,
            description="Suffix to append to table names",
        ),
        th.Property(
            "warning_if_no_files",
            th.BooleanType,
            description="Warning instead of error if no files found",
        ),
        th.Property(
            "s3_proxies",
            th.ObjectType(
                th.Property("http", th.StringType),
                th.Property("https", th.StringType),
            ),
            description="Proxy settings for S3",
        ),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        """Initialize the tap."""
        # Create logging config file
        log_dir = os.path.join(os.getcwd(), ".meltano", "run", "tap-gn-s3")
        os.makedirs(log_dir, exist_ok=True)
        log_config_path = os.path.join(log_dir, "tap.singer_sdk_logging.json")
        
        with open(log_config_path, "w") as f:
            json.dump(DEFAULT_LOGGING, f)

        super().__init__(*args, **kwargs)

    def discover_streams(self) -> list[CSVFileStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            CSVFileStream(self, table_spec)
            for table_spec in self.config["tables"]
        ]


if __name__ == "__main__":
    TapGnS3.cli()
