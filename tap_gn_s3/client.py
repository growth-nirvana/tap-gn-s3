"""REST client handling, including S3CSVStream base class."""

from __future__ import annotations

import copy
import csv
import sys
from typing import Dict, Generator, Iterator, List, Optional

from singer import get_logger, utils
from singer_encodings.csv import (  # pylint:disable=no-name-in-module
    get_row_iterator,
)
from singer_sdk.streams import Stream

from tap_gn_s3.s3_client import S3Client, SDC_SOURCE_BUCKET_COLUMN, SDC_SOURCE_FILE_COLUMN, SDC_SOURCE_LINENO_COLUMN

LOGGER = get_logger("tap_gn_s3")


class S3CSVStream(Stream):
    """S3 CSV stream class."""

    def __init__(self, tap, table_spec: Dict):
        """Initialize the stream.

        Args:
            tap: The tap instance
            table_spec: Table specification
        """
        super().__init__(tap)
        self.table_spec = table_spec
        self.s3_client = S3Client(tap.config)
        self._name = table_spec["table_name"] + tap.config.get("table_suffix", "")

    @property
    def name(self) -> str:
        """Return the stream name."""
        return self._name

    @property
    def key_properties(self) -> List[str]:
        """Return the key properties for the stream."""
        return self.table_spec.get("key_properties", [])

    def _sync(self) -> None:
        """Sync the stream."""
        table_name = self.table_spec["table_name"] + self.config.get("table_suffix", "")
        modified_since = utils.strptime_with_tz(
            self.get_starting_timestamp("modified_since") or self.config["start_date"]
        )

        LOGGER.info('Syncing table "%s".', table_name)
        LOGGER.info("Getting files modified since %s.", modified_since)

        s3_files = self.s3_client.get_input_files_for_table(
            self.table_spec, modified_since
        )

        records_streamed = 0

        # We sort here so that tracking the modified_since bookmark makes
        # sense. This means that we can't sync s3 buckets that are larger than
        # we can sort in memory which is suboptimal. If we could bookmark
        # based on anything else then we could just sync files as we see them.
        for s3_file in sorted(s3_files, key=lambda item: item["last_modified"]):
            records_streamed += self._sync_table_file(s3_file["key"])

            self.update_starting_timestamp(
                "modified_since", s3_file["last_modified"].isoformat()
            )

        LOGGER.info('Wrote %s records for table "%s".', records_streamed, table_name)

    def _sync_table_file(self, s3_path: str) -> int:
        """Sync a given csv found file.

        Args:
            s3_path: file path given by S3

        Returns:
            number of streamed records
        """
        LOGGER.info('Syncing file "%s".', s3_path)

        bucket = self.config["bucket"]
        table_name = self.table_spec["table_name"] + self.config.get("table_suffix", "")

        s3_file_handle = self.s3_client.get_file_handle(s3_path)
        # We observed data who's field size exceeded the default maximum of
        # 131072. We believe the primary consequence of the following setting
        # is that a malformed, wide CSV would potentially parse into a single
        # large field rather than giving this error, but we also think the
        # chances of that are very small and at any rate the source data would
        # need to be fixed. The other consequence of this could be larger
        # memory consumption but that's acceptable as well.
        csv.field_size_limit(sys.maxsize)
        iterator = get_row_iterator(
            s3_file_handle._raw_stream, self.table_spec
        )  # pylint:disable=protected-access

        records_synced = 0

        for row in iterator:
            time_extracted = utils.now()

            custom_columns = {
                SDC_SOURCE_BUCKET_COLUMN: bucket,
                SDC_SOURCE_FILE_COLUMN: s3_path,
                # index zero, +1 for header row
                SDC_SOURCE_LINENO_COLUMN: records_synced + 2,
            }
            if self.config.get("set_empty_values_null", False):
                row = self._set_empty_values_null(row)

            rec = {**row, **custom_columns}

            # Transform column names to be BigQuery compliant
            rec = self._transform_column_names(rec)

            self.write_record(rec, time_extracted=time_extracted)
            records_synced += 1

        return records_synced

    def _set_empty_values_null(self, input_row):
        """Set empty values to null.

        Args:
            input_row: Input row

        Returns:
            Row with empty values set to null
        """
        ret = copy.deepcopy(input_row)
        # Handle dictionaries, lists & tuples. Scrub all values
        if isinstance(input_row, dict):
            for dict_key, dict_value in ret.items():
                ret[dict_key] = self._set_empty_values_null(dict_value)
        if isinstance(input_row, (list, tuple)):
            for dict_key, dict_value in enumerate(ret):
                ret[dict_key] = self._set_empty_values_null(dict_value)
        # If value is empty or all spaces convert to None
        if input_row == "" or str(input_row).isspace():
            ret = None
        # Finished scrubbing
        return ret

    def _transform_column_names(self, record: Dict) -> Dict:
        """Transform column names to be BigQuery compliant.

        Args:
            record: Input record

        Returns:
            Record with transformed column names
        """
        transformed = {}
        for key, value in record.items():
            # Skip source tracking columns
            if key.startswith("_sdc_"):
                transformed[key] = value
                continue

            # Transform column name to be BigQuery compliant
            transformed_key = key.lower()
            # Replace spaces and special characters with underscores
            transformed_key = "".join(
                c if c.isalnum() else "_" for c in transformed_key
            )
            # Remove consecutive underscores
            transformed_key = re.sub(r"_+", "_", transformed_key)
            # Remove leading/trailing underscores
            transformed_key = transformed_key.strip("_")
            # Ensure the key is not empty
            if not transformed_key:
                transformed_key = "unnamed_column"

            transformed[transformed_key] = value

        return transformed
