# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>

from __future__ import absolute_import, unicode_literals

import logging
import os
import tempfile
from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_salesforce_plugin.hooks.salesforce_hook import SalesforceHook


class SalesforceToFileOperator(BaseOperator):

    """
    Writes results of a Salesforce soql statement to a given filepath in csv format.
    """

    template_fields = ("soql", "soql_args", "filepath")

    @apply_defaults
    def __init__(
        self,
        conn_id: str,
        soql: str = None,
        soql_args: str = None,
        filepath: str = None,
        *args,
        **kwargs,
    ) -> None:
        """
        Build an instance of the Salesforce -> File operator.

        .. note:: If no filepath is given then a temporary filepath is created.

        :param conn_id: The HTTP or Direct connection ids in Airflow for
            Salesforce
        :type conn_id: str
        :param soql: The SOQL statement to execute
        :type soql: str
        :param soql_args: A string of comma delimited SOQL args to use for SOQL
        :type soql_args: str
        :param filepath: The filepath to write results to, defaults to None
        :param filepath: str, optional
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)
        # NOTE: template fields do not get applied until call to `self.execute`
        self.conn_id = conn_id
        self.soql = soql
        self.soql_args = soql_args
        self.filepath = filepath
        self._hook = None

    @property
    def hook(self):
        """Create or retrieve the hook required to use the current operator"""
        if not self._hook:
            self.log.info(f"Creating Salesforce Hook for connection {self.conn_id}")
            self._hook = SalesforceHook(self.conn_id)
        return self._hook

    @hook.setter
    def hook(self, val):
        self._hook = val

    def execute(self, context: Dict[str, Any]) -> str:
        """
        Executes the given soql statement and writes the results to a csv file.

        :param context: The context of the running DAG
        :type context: Dict[str, Any]
        :returns: The filepath to which results were written to
        :rtype: str
        """
        if not isinstance(self.filepath, str):
            # generate temporary if no filepath given
            filepath = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
            self.filepath = filepath.name
            self.log.debug(
                f"no filepath given, creating temporary file at {self.filepath!r}"
            )

        statement = self.soql % tuple(self.soql_args.split(","))
        self.log.info(
            f"exporting data from executing {statement!r} on "
            f"{self.hook!r} to {self.filepath!r}"
        )
        self.hook.export(self.soql, self.filepath, self.soql_args.split(","), True)
        return self.filepath
