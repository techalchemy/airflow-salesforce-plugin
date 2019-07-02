# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>

from __future__ import absolute_import, unicode_literals

import json
import logging
import os
import pathlib
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_salesforce_plugin.hooks.salesforce_hook import SalesforceHook


class SalesforceToS3Operator(BaseOperator):

    """
    Writes results of a Salesforce soql statement to a given S3 Bucket.
    """

    template_fields = (
        "soql",
        "soql_args",
        "s3_conn_id",
        "s3_bucket",
        "upload_timeout",
        "object_name",
    )

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        soql: str = None,
        soql_args: str = None,
        s3_conn_id: str = None,
        s3_bucket: str = None,
        object_key: str = None,
        upload_timeout: int = 15,
        *args,
        **kwargs,
    ) -> None:
        """
        Build an instance of the Salesforce -> Attachment operator.

        :param conn_id: The HTTP or Direct connection id for Salesforce
        :type conn_id: str
        :param soql: The SOQL statement to execute (templated)
        :type soql: str
        :param soql_args: A string of comma delimited SOQL args to use (templated)
        :type soql_args: str
        :param s3_conn_id: The S3 Connection ID (templated)
        :type s3_conn_id: str
        :param s3_bucket: The S3 Bucket ID (templated)
        :type s3_bucket: str
        :param object_key: Name of the target S3 key (templated)
        :type object_key: str
        :param upload_timeout: Upload timeout, default 15 (templated)
        :type upload_timeout: int
        """

        super(SalesforceToS3Operator, self).__init__(*args, **kwargs)
        # NOTE: template fields do not get applied until call to `self.execute`
        self.conn_id = conn_id
        self.soql = soql
        self.soql_args = soql_args
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.upload_timeout = upload_timeout
        self.object_key = object_key

    def execute(self, context: Dict[str, Any]) -> List[str]:
        """
        Executes the given context to extract attachments and load them into S3.

        :param context: The context of the running DAG
        :type context: Dict[str, Any]
        :returns: The S3 keys of the loaded attachments
        :rtype: List[str]
        """

        sf_hook = SalesforceHook(self.conn_id)
        filepath = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        self.filepath = filepath.name

        resultfile = sf_hook.export(
            self.soql % tuple(self.soql_args.split(",")), self.filepath
        )
        self._upload_file(resultfile)

    def _upload_file(self, resultfile: str) -> str:
        s3_hook = S3Hook(self.s3_conn_id)
        record_key = self.object_key
        if s3_hook.check_for_key(record_key, bucket_name=self.s3_bucket):
            self.log.warning(
                f"overwriting existing file for {record_key!r} in {s3_hook!r}"
            )
        else:
            self.log.info(
                f"creating object from {resultfile!r} at {record_key!r} in {s3_hook!r}"
            )

        try:
            attachment_content = pathlib.Path(resultfile).read_bytes()
        except Exception as exc:
            raise AirflowException(f"error occured on file from {resultfile!r}, {exc!s}")
        try:
            s3_hook.load_bytes(attachment_content, record_key, bucket_name=self.s3_bucket)
        except Exception as exc:
            raise AirflowException(
                f"error occured while trying to upload attachment from {resultfile!r}"
                f" to key {record_key!r} in {s3_hook!r}, {exc}"
            )
        return record_key
