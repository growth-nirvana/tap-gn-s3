"""Stream type classes for tap-gn-s3."""

from __future__ import annotations

import csv
import sys
import re
import io
import zipfile
import itertools
import more_itertools
import pendulum
from typing import Any, Dict, Iterable, Optional, List, Tuple

from singer_sdk import Stream, Tap
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType,
    IntegerType,
    ArrayType,
)

from tap_gn_s3.client import S3Client
from tap_gn_s3.csv_handler import get_row_iterator

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_EXTRA_COLUMN = "_sdc_extra"

class CSVFileStream(Stream):
    """Stream for reading CSV files from S3."""

    # Add replication key for state management
    replication_key = "_gn_last_modified"
    is_timestamp_replication_key = True
    primary_keys = ["_gn_file_path", "_gn_row_number"]  # Add row_number to uniquely identify each record

    def __init__(
        self,
        tap: Tap,
        table_spec: Dict,
    ):
        """Initialize the stream.

        Args:
            tap: The Tap instance
            table_spec: The table specification
        """
        self.table_spec = table_spec
        name = table_spec["table_name"]
        
        # Initialize client
        self.s3_client = S3Client(tap.config)
        
        # Initialize parent class
        super().__init__(tap, name=name)

    def _sanitize_column_name(self, column_name: str) -> str:
        """Make column name BigQuery compliant.
        
        Args:
            column_name: Original column name
            
        Returns:
            Sanitized column name
        """
        # Replace spaces and special chars with underscore
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
        # Replace multiple underscores with single underscore
        sanitized = re.sub(r'_+', '_', sanitized)
        # Ensure starts with letter or underscore
        if not sanitized[0].isalpha() and sanitized[0] != '_':
            sanitized = f'_{sanitized}'
        # Convert to lowercase for consistency
        return sanitized.lower()

    def _get_column_mapping(self, headers: set[str]) -> Dict[str, str]:
        """Get mapping of original to sanitized column names.
        
        Args:
            headers: Set of original headers
            
        Returns:
            Dictionary mapping original to sanitized names
        """
        return {
            header: self._sanitize_column_name(header)
            for header in headers
        }

    def _sample_file(self, file_path: str, sample_rate: int = 5) -> Iterable[Dict]:
        """Sample a single file to detect schema.
        
        Args:
            file_path: S3 file path
            sample_rate: Sample every Nth row
            
        Returns:
            Iterator of sampled rows
        """
        file_handle = self.s3_client.get_file_handle(file_path)
        iterator = get_row_iterator(file_handle._raw_stream, self.table_spec)

        current_row = 0
        sampled_row_count = 0

        for row in iterator:
            if (current_row % sample_rate) == 0:
                if row.get(SDC_EXTRA_COLUMN):
                    row.pop(SDC_EXTRA_COLUMN)
                sampled_row_count += 1
                if (sampled_row_count % 200) == 0:
                    self.logger.info("Sampled %s rows from %s", sampled_row_count, file_path)
                yield row
            current_row += 1

        self.logger.info("Sampled %s rows from %s", sampled_row_count, file_path)

    def _sample_files(self, s3_files: List[Dict], sample_rate: int = 5, max_records: int = 1000, max_files: int = 5) -> Iterable[Dict]:
        """Sample multiple files to detect schema.
        
        Args:
            s3_files: List of S3 file info
            sample_rate: Sample every Nth row
            max_records: Maximum records to sample per file
            max_files: Maximum number of files to sample
            
        Returns:
            Iterator of sampled rows
        """
        self.logger.info("Sampling files (max files: %s)", max_files)
        for s3_file in more_itertools.tail(max_files, s3_files):
            self.logger.info(
                'Sampling %s (max records: %s, sample rate: %s)',
                s3_file['key'],
                max_records,
                sample_rate
            )
            yield from itertools.islice(
                self._sample_file(s3_file['key'], sample_rate),
                max_records
            )

    @property
    def schema(self) -> dict:
        """Get stream schema.

        Returns:
            Stream schema.
        """
        if not hasattr(self, '_schema'):
            self.logger.info('Sampling records to determine table schema.')

            # Get files to sample
            s3_files = self.s3_client.get_input_files_for_table(
                self.table_spec,
                None  # Don't filter by modified_since for schema detection
            )

            # Sample files to detect schema
            samples = list(self._sample_files(s3_files))
            
            if not samples:
                self._schema = {}
                return self._schema

            # Build schema from samples
            date_overrides = set(self.table_spec.get('date_overrides', []))

            # Get all unique headers and create column mapping
            headers = set()
            for sample in samples:
                headers.update(sample.keys())
            
            self._column_mapping = self._get_column_mapping(headers)

            # Create schema properties
            properties = {
                # Add standard fields that all CSV files should have
                SDC_SOURCE_BUCKET_COLUMN: Property(SDC_SOURCE_BUCKET_COLUMN, StringType, required=True),
                SDC_SOURCE_FILE_COLUMN: Property(SDC_SOURCE_FILE_COLUMN, StringType, required=True),
                SDC_SOURCE_LINENO_COLUMN: Property(SDC_SOURCE_LINENO_COLUMN, IntegerType, required=True),
                SDC_EXTRA_COLUMN: Property(SDC_EXTRA_COLUMN, ArrayType(StringType)),
                # Add _gn_last_modified for replication key
                "_gn_last_modified": Property("_gn_last_modified", DateTimeType, required=True),
                # Add primary key fields with _gn prefix
                "_gn_file_path": Property("_gn_file_path", StringType, required=True),
                "_gn_row_number": Property("_gn_row_number", IntegerType, required=True),
            }

            # Add data fields with sanitized names
            for header in headers:
                sanitized_name = self._column_mapping[header]
                if header in date_overrides:
                    properties[sanitized_name] = Property(sanitized_name, DateTimeType)
                else:
                    properties[sanitized_name] = Property(sanitized_name, StringType)

            self._schema = PropertiesList(*properties.values()).to_dict()

        return self._schema

    def _is_zip_file(self, file_handle) -> bool:
        """Check if file is a ZIP archive.
        
        Args:
            file_handle: File handle to check
            
        Returns:
            True if file is ZIP archive
        """
        # Read first 4 bytes to check ZIP magic number
        try:
            header = file_handle._raw_stream.read(4)
            file_handle._raw_stream.seek(0)  # Reset position
            return header.startswith(b'PK\x03\x04')
        except:
            return False

    def _process_zip_file(self, zip_path: str, file_handle, last_modified: str) -> Iterable[Tuple[str, Any]]:
        """Process a ZIP file containing CSVs.
        
        Args:
            zip_path: Path to ZIP file in S3
            file_handle: File handle for ZIP
            last_modified: Last modified timestamp
            
        Yields:
            Tuple of (file_path, csv_reader) for each CSV in ZIP
        """
        # Create ZIP file object from raw stream
        zip_data = io.BytesIO(file_handle._raw_stream.read())
        with zipfile.ZipFile(zip_data) as zip_file:
            # Process each file in ZIP
            for zip_info in zip_file.filelist:
                # Skip directories and non-CSV files
                if zip_info.filename.endswith('/') or not zip_info.filename.lower().endswith('.csv'):
                    continue
                    
                # Construct full path including ZIP
                full_path = f"{zip_path}::{zip_info.filename}"
                self.logger.info(f"Processing CSV from ZIP: {full_path}")
                
                # Open CSV from ZIP
                with zip_file.open(zip_info) as csv_file:
                    # Wrap in TextIOWrapper for CSV reader
                    text_stream = io.TextIOWrapper(csv_file, encoding='utf-8')
                    csv.field_size_limit(sys.maxsize)
                    reader = get_row_iterator(text_stream, self.table_spec)
                    yield full_path, reader

    def get_records(self, context: Optional[dict] = None) -> Iterable[Dict[str, Any]]:
        """Get records from the stream.

        Args:
            context: The context for the stream.

        Yields:
            A dictionary for each record.
        """
        bucket = self.config["bucket"]
        start_time = self.get_starting_replication_key_value(context)
        
        s3_files = self.s3_client.get_input_files_for_table(
            self.table_spec,
            start_time if start_time else None
        )

        for s3_file in sorted(s3_files, key=lambda item: item["last_modified"]):
            file_path = s3_file["key"]
            last_modified = s3_file["last_modified"].isoformat()
            self.logger.info(f"🔍 Processing file: {file_path} | Last Modified: {last_modified}")

            file_handle = self.s3_client.get_file_handle(file_path)
            
            # Check if file is ZIP archive
            if self._is_zip_file(file_handle):
                # Process each CSV in ZIP
                for csv_path, reader in self._process_zip_file(file_path, file_handle, last_modified):
                    for row_number, row in enumerate(reader, start=1):
                        extra_columns = row.pop(SDC_EXTRA_COLUMN, None)
                        sanitized_row = {
                            self._column_mapping.get(k, k): v 
                            for k, v in row.items()
                        }
                        yield {
                            SDC_SOURCE_BUCKET_COLUMN: bucket,
                            SDC_SOURCE_FILE_COLUMN: csv_path,  # Use full path including ZIP structure
                            SDC_SOURCE_LINENO_COLUMN: row_number,
                            SDC_EXTRA_COLUMN: extra_columns,
                            "_gn_last_modified": last_modified,
                            "_gn_file_path": csv_path,
                            "_gn_row_number": row_number,
                            **sanitized_row,
                        }
            else:
                # Process regular CSV file
                csv.field_size_limit(sys.maxsize)
                reader = get_row_iterator(file_handle._raw_stream, self.table_spec)
                for row_number, row in enumerate(reader, start=1):
                    extra_columns = row.pop(SDC_EXTRA_COLUMN, None)
                    sanitized_row = {
                        self._column_mapping.get(k, k): v 
                        for k, v in row.items()
                    }
                    yield {
                        SDC_SOURCE_BUCKET_COLUMN: bucket,
                        SDC_SOURCE_FILE_COLUMN: file_path,
                        SDC_SOURCE_LINENO_COLUMN: row_number,
                        SDC_EXTRA_COLUMN: extra_columns,
                        "_gn_last_modified": last_modified,
                        "_gn_file_path": file_path,
                        "_gn_row_number": row_number,
                        **sanitized_row,
                    }
