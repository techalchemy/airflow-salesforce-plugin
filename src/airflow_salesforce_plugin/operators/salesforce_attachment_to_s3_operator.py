# -*- coding=utf-8 -*-
# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>

from __future__ import absolute_import, unicode_literals

import concurrent.futures
import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_salesforce_plugin.hooks.salesforce_hook import SalesforceHook


class SalesforceAttachmentToS3Operator(BaseOperator):

    """
    Writes results of a Salesforce soql statement to a given S3 Bucket.
    """

    template_fields = (
        "attachment_ids",
        "s3_conn_id",
        "s3_bucket",
        "upload_timeout",
        "concurrent_uploads",
        "api_version",
    )

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        attachment_ids: str = None,
        s3_conn_id: str = None,
        s3_bucket: str = None,
        upload_timeout: int = 15,
        concurrent_uploads: int = 5,
        api_version: str = "v38.0",
        provide_context: bool = False,
        *args,
        **kwargs,
    ) -> None:
        """
        Build an instance of the Salesforce -> Attachment operator.

        :param conn_id: The HTTP or Direct connection ids in Airflow for
            Salesforce
        :type conn_id: str
        :param str attachment_ids: A comma delimited string of attachment ids
        :param s3_conn_id: The S3 Connection ID
        :type s3_conn_id: str
        :param s3_bucket: The S3 Bucket ID
        :type s3_bucket: str
        :param upload_timeout: Upload timeout, default 15
        :type upload_timeout: int
        :param concurrent_uploads: Number of concurrent uploads to perform, default 15
        :param bool provide_context: Whether to include context, default False
        :type concurrent_uploads: int
        :param str api_version: The salesforce API version to use, defaults to v38.0
        """

        super(SalesforceAttachmentToS3Operator, self).__init__(*args, **kwargs)
        # NOTE: template fields do not get applied until call to `self.execute`
        self.conn_id = conn_id
        self.attachment_ids = attachment_ids
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.upload_timeout = upload_timeout
        self.concurrent_uploads = concurrent_uploads
        self.api_version = api_version
        self.provide_context = provide_context

    def execute(self, context: Dict[str, Any]) -> List[str]:
        """
        Executes the given context to extract attachments and load them into S3.

        :param context: The context of the running DAG
        :type context: Dict[str, Any]
        :returns: The S3 keys of the loaded attachments
        :rtype: List[str]
        """

        attachment_id_list: List[str] = []
        if self.attachment_ids and "," in self.attachment_ids:
            attachment_id_list = self.attachment_ids.split(",")
        id_dict = {"record_ids": json.dumps({"ids": attachment_id_list})}
        return self._handle_attachments(templates_dict=id_dict)

    def _upload_attachment(self, record_id: str) -> Optional[str]:
        sf_hook = SalesforceHook(self.conn_id).login()
        s3_hook = S3Hook(self.s3_conn_id)
        record_url = (
            f"https://{sf_hook.session.sf_instance}/services/data/{self.api_version}"
            f"/sobjects/Attachment/{record_id}/Body"
        )
        record_key = f"attachment/salesforce/{record_id}"

        if not s3_hook.check_for_key(record_key, bucket_name=self.s3_bucket):
            self.log.info(
                f"creating object from {record_url!r} at {record_key!r} in {s3_hook!r}"
            )
            try:
                attachment_content = sf_hook.session.request.get(
                    record_url,
                    headers=sf_hook.session.headers,
                    timeout=self.upload_timeout,
                ).content
            except Exception as exc:
                raise AirflowException(
                    f"error occured on attachment from {record_url!r}, {exc!s}"
                )
            try:
                s3_hook.load_bytes(
                    attachment_content, record_key, bucket_name=self.s3_bucket
                )
            except Exception as exc:
                raise AirflowException(
                    f"error occured while trying to uplod attachment from {record_url!r}"
                    f" to key {record_key!r} in {s3_hook!r}, {exc}"
                )
            return record_key
        else:
            self.log.info(
                f"skipping creation for object with key {record_key!r} in {s3_hook!r}, "
                "already exists"
            )
            return None

    def _handle_attachments(
        self, templates_dict: Dict[str, str] = None, **kwargs
    ) -> List[str]:
        if templates_dict is None:
            templates_dict = {}
        record_id_string = templates_dict.get("record_ids")
        results: List[str] = []
        if record_id_string is not None:
            self.log.info(f"Using id string to get attachments: {record_id_string}")
            record_ids = json.loads(record_id_string).get("ids", [])
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.concurrent_uploads
            ) as executor:
                result_map = executor.map(self._upload_attachment, record_ids)
                results = [r for r in result_map if r is not None]
        return results
