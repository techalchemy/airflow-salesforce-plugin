# -*- coding: utf-8 -*-
import pathlib
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import vcr

from airflow_salesforce_plugin.hooks import SalesforceHook

from .test_utils import COLUMNS, _sync_interval


@pytest.mark.parametrize("table, columns", COLUMNS)
def test_sql_parser(sql_dir, salesforce_hook, table, columns):
    soql_file = sql_dir / f"{table}.sql"
    soql = soql_file.read_text()
    parsed = [
        col.replace(".", "_").lower()
        for col in salesforce_hook._extract_columns(soql, replace={".": "_"})
    ]
    assert parsed == columns


def test_export(soql_params, salesforce_hook, csv_dir, sql_file, tmpdir):
    statement = sql_file.sql_text % tuple(soql_params)
    csv_filename = f"{sql_file.name}.csv"
    filepath = csv_dir / csv_filename

    class MockFile(object):
        def __init__(self, file_, *args, **kwargs):
            self.file = file_
            super().__init__(*args, **kwargs)

        def iter_lines(self):
            for chunk in iter(lambda: self.file.readline(), b""):
                yield chunk

        def __getattr__(self, attrib):
            file_ = super().__getattribute__("file")
            if attrib == "text":
                return file_.read()
            return getattr(file_, attrib)

    target = tmpdir.join(csv_filename)
    with patch.object(salesforce_hook, "login"):
        with open(sql_file.json_path.as_posix(), "r") as fh:
            salesforce_hook.session = MagicMock()
            salesforce_hook._get_results = MagicMock(return_value=MockFile(fh))
            salesforce_hook.export(sql_file.sql_text, target.strpath, soql_params)
