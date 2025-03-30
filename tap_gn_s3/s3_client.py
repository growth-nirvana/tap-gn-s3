"""S3 client for handling S3 operations."""

from __future__ import annotations

import itertools
import os
import re
from typing import Dict, Generator, Iterator, List, Optional

import backoff
import boto3
import more_itertools
from botocore.config import Config
from botocore.exceptions import ClientError
from singer import get_logger, utils
from singer_encodings.csv import (  # pylint:disable=no-name-in-module
    SDC_EXTRA_COLUMN,
    get_row_iterator,
)

LOGGER = get_logger("tap_gn_s3")

SDC_SOURCE_BUCKET_COLUMN = "_sdc_source_bucket"
SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"


def retry_pattern():
    """Retry decorator to retry failed functions."""
    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=log_backoff_attempt,
        factor=10,
    )


def log_backoff_attempt(details):
    """For logging attempts to connect with Amazon."""
    LOGGER.info(
        "Error detected communicating with Amazon, triggering backoff: %d try",
        details.get("tries"),
    )


class S3Client:
    """Client for handling S3 operations."""

    def __init__(self, config: Dict):
        """Initialize the S3 client.

        Args:
            config: Tap configuration
        """
        self.config = config
        self._setup_aws_client()

    @retry_pattern()
    def _setup_aws_client(self) -> None:
        """Initialize a default AWS session."""
        LOGGER.info("Attempting to create AWS session")

        # Get the required parameters from config file and/or environment variables
        aws_access_key_id = self.config.get("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        aws_secret_access_key = self.config.get(
            "aws_secret_access_key"
        ) or os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_session_token = self.config.get("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )
        aws_profile = self.config.get("aws_profile") or os.environ.get("AWS_PROFILE")

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            boto3.setup_default_session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )
        # AWS Profile based authentication
        else:
            boto3.setup_default_session(profile_name=aws_profile)

    @retry_pattern()
    def list_files_in_bucket(
        self,
        bucket: str,
        search_prefix: Optional[str] = None,
        aws_endpoint_url: Optional[str] = None,
        s3_proxies: Optional[dict] = None,
    ) -> Generator:
        """Get all files in the given S3 bucket that match the search prefix.

        Args:
            bucket: S3 bucket name
            search_prefix: search pattern
            aws_endpoint_url: optional aws url
            s3_proxies: optional dict of proxies

        Returns:
            Generator containing all found files
        """
        # override default endpoint for non aws s3 services
        if aws_endpoint_url is not None:
            if s3_proxies is None:
                s3_client = boto3.client("s3", endpoint_url=aws_endpoint_url)
            else:
                s3_client = boto3.client(
                    "s3",
                    endpoint_url=aws_endpoint_url,
                    config=Config(proxies=s3_proxies),
                )
        else:
            if s3_proxies is None:
                s3_client = boto3.client("s3")
            else:
                s3_client = boto3.client("s3", config=Config(proxies=s3_proxies))

        s3_object_count = 0
        max_results = 1000
        args = {
            "Bucket": bucket,
            "MaxKeys": max_results,
        }

        if search_prefix is not None:
            args["Prefix"] = search_prefix

        paginator = s3_client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(**args)
        filtered_s3_objects = page_iterator.search(
            "Contents[?StorageClass=='STANDARD']"
        )

        for s3_obj in filtered_s3_objects:
            s3_object_count += 1
            LOGGER.debug("On page %s", s3_object_count)
            yield s3_obj

        if s3_object_count > 0:
            LOGGER.info("Found %s files.", s3_object_count)
        else:
            LOGGER.warning(
                'Found no files for bucket "%s" that match prefix "%s"',
                bucket,
                search_prefix,
            )

    @retry_pattern()
    def get_file_handle(self, s3_path: str) -> Iterator:
        """Get a iterator of file located in the s3 path.

        Args:
            s3_path: file path in S3

        Returns:
            file Body iterator
        """
        bucket = self.config["bucket"]
        aws_endpoint_url = self.config.get("aws_endpoint_url")
        s3_proxies = self.config.get("s3_proxies")

        # override default endpoint for non aws s3 services
        if aws_endpoint_url is not None:
            if s3_proxies is None:
                s3_client = boto3.resource("s3", endpoint_url=aws_endpoint_url)
            else:
                s3_client = boto3.resource(
                    "s3",
                    endpoint_url=aws_endpoint_url,
                    config=Config(proxies=s3_proxies),
                )
        else:
            if s3_proxies is None:
                s3_client = boto3.resource("s3")
            else:
                s3_client = boto3.resource("s3", config=Config(proxies=s3_proxies))

        s3_bucket = s3_client.Bucket(bucket)
        s3_object = s3_bucket.Object(s3_path)
        return s3_object.get()["Body"]

    def get_input_files_for_table(
        self, table_spec: Dict, modified_since: Optional[str] = None
    ) -> Generator:
        """Get all files that match the search pattern in table specs.

        Args:
            table_spec: table specs
            modified_since: string date

        Returns:
            Generator containing all the found files
        """
        bucket = self.config["bucket"]
        warning_if_no_files = self.config.get("warning_if_no_files", False)

        prefix = table_spec.get("search_prefix")
        pattern = table_spec["search_pattern"]
        try:
            matcher = re.compile(pattern)
        except re.error as err:
            raise ValueError(
                (
                    f"search_pattern for table `{table_spec['table_name']}` "
                    "is not a valid regular "
                    "expression. See "
                    "https://docs.python.org/3.5/library/re.html"
                    "#regular-expression-syntax"
                ),
                pattern,
            ) from err

        LOGGER.info('Checking bucket "%s" for keys matching "%s"', bucket, pattern)
        LOGGER.info(
            "Skipping files which have a LastModified value older than %s",
            modified_since,
        )

        matched_files_count = 0
        unmatched_files_count = 0
        max_files_before_log = 30000
        for s3_object in sorted(
            self.list_files_in_bucket(
                bucket,
                prefix,
                aws_endpoint_url=self.config.get("aws_endpoint_url"),
                s3_proxies=self.config.get("s3_proxies"),
            ),
            key=lambda item: item["LastModified"],
            reverse=False,
        ):
            key = s3_object["Key"]
            last_modified = s3_object["LastModified"]

            if s3_object["Size"] == 0:
                LOGGER.info('Skipping matched file "%s" as it is empty', key)
                unmatched_files_count += 1
                continue

            if matcher.search(key):
                matched_files_count += 1
                if modified_since is None or modified_since < last_modified:
                    LOGGER.info(
                        'Will download key "%s" as it was last modified %s',
                        key,
                        last_modified,
                    )
                    yield {"key": key, "last_modified": last_modified}
            else:
                unmatched_files_count += 1

            if (
                unmatched_files_count + matched_files_count
            ) % max_files_before_log == 0:
                # Are we skipping greater than 50% of the files?
                if (
                    unmatched_files_count
                    / (matched_files_count + unmatched_files_count)
                ) > 0.5:
                    LOGGER.warning(
                        (
                            "Found %s matching files and %s non-matching files. "
                            "You should consider adding a `search_prefix` "
                            "to the config "
                            "or removing non-matching files from the bucket."
                        ),
                        matched_files_count,
                        unmatched_files_count,
                    )
                else:
                    LOGGER.info(
                        "Found %s matching files and %s non-matching files",
                        matched_files_count,
                        unmatched_files_count,
                    )

        if matched_files_count == 0:
            if warning_if_no_files:
                LOGGER.warning(
                    f'No files found in bucket "{bucket}" that matches prefix "'
                    '{prefix}" and pattern "{pattern}"'
                )
            else:
                if prefix:
                    raise Exception(
                        f'No files found in bucket "{bucket}" that matches prefix '
                        '"{prefix}" and pattern "{pattern}"'
                    )

                raise Exception(
                    f'No files found in bucket "{bucket}" that matches pattern '
                    '"{pattern}"'
                ) 