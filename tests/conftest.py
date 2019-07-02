# -*- coding: utf-8 -*-
import os
import pathlib
from datetime import datetime
from typing import Any, NamedTuple
from unittest.mock import Mock, patch

import pytest
import requests
import requests_mock
from airflow import configuration
from airflow.models import Connection
from hypothesis import assume, given, strategies as st

from airflow_salesforce_plugin.hooks.salesforce_hook import SalesforceHook

from .strategies import allowed_date_range

TEST_ROOT = pathlib.Path(__file__).absolute().parent
FIXTURE_DIR = TEST_ROOT / "fixtures"


class SoqlFile(NamedTuple):
    path: pathlib.Path
    name: str
    sql_text: str
    json_path: pathlib.Path
    csv_path: pathlib.Path


class Operator(NamedTuple):
    operator: Any
    target: str


@pytest.fixture(autouse=True)
def load_test_config():
    old_fernet_key = os.environ.get("FERNET_KEY", None)
    os.environ["FERNET_KEY"] = "test_fernet_key"
    configuration.load_test_config()
    if old_fernet_key:
        os.environ["FERNET_KEY"] = old_fernet_key


@pytest.fixture()
def airflow_connection():
    return Connection(
        conn_id="http_default",
        conn_type="http",
        host="test:8080/",
        extra='{"bareer": "test"}',
    )


def get_salesforce_connection(conn_id="salesforce_default"):
    return Connection(
        conn_id=conn_id,
        conn_type="http",
        host="http://sf_test:8081/",
        login="fake_user",
        password="fake_password",
        extra='{"security_token": "fake_token", "sandbox": "1"}',
    )


@pytest.fixture()
def salesforce_connection():
    return get_salesforce_connection()


@requests_mock.mock()
@patch("simple_salesforce.api.SalesforceLogin", return_value=("12345", "test"))
@patch(
    "airflow_salesforce_plugin.hooks.salesforce_hook.SalesforceHook.get_connection",
    side_effect=get_salesforce_connection,
)
def get_salesforce_hook(m, login_mock, connection_mock):
    session = requests.Session()
    adapter = requests_mock.Adapter()
    session.mount("mock", adapter)
    sf_hook = SalesforceHook()
    sf_hook.session = session
    return sf_hook


@pytest.fixture()
def salesforce_hook(salesforce_connection):
    hook = get_salesforce_hook()
    yield hook


@pytest.fixture()
def salesforce_staging_hook():
    sf_hook = SalesforceHook("salesforce_cx_staging")
    sf_hook.login()
    return sf_hook


@pytest.fixture()
def airflow_connection_with_port(conn_id=None):
    return Connection(
        conn_id="http_default", conn_type="http", host="test.com", port=1234
    )


@pytest.fixture(scope="session")
def soql_params():
    min_date, max_date = allowed_date_range().example()
    soql_args = [
        f"{min_date.replace(tzinfo=None).isoformat()}Z",
        f"{max_date.replace(tzinfo=None).isoformat()}Z",
    ]
    return soql_args


@pytest.fixture
def sql_dir():
    return FIXTURE_DIR / "sql"


@pytest.fixture
def csv_dir():
    return FIXTURE_DIR / "csvs"


@pytest.fixture
def db_dir():
    return FIXTURE_DIR / "db"


@pytest.fixture
def sql(sql_dir, request):
    table = request.get("table", None)
    if table is not None:
        sql_file = sql_dir / f"{table}.sql"
        return sql_dir.read_text()
    return None


def _get_sql_files():
    files = []
    for sql_file in FIXTURE_DIR.joinpath("sql").glob("*.sql"):
        constructed = SoqlFile(
            sql_file,
            sql_file.stem,
            sql_file.read_text(),
            FIXTURE_DIR / f"http/{sql_file.stem}.json",
            FIXTURE_DIR / f"csvs/{sql_file.stem}",
        )
        files.append(constructed)
    return files


@pytest.fixture(params=_get_sql_files())
def sql_file(request):
    return request.param


@pytest.fixture()
def salesforce_to_file_operator(soql_params, sql_file, tmpdir):
    csv_filename = f"{sql_file.name}.csv"
    target = tmpdir.join(csv_filename)
    soql_params = ",".join(soql_params)
    with patch(
        "airflow.hooks.base_hook.BaseHook.get_connection",
        side_effect=get_salesforce_connection,
    ):
        from airflow_salesforce_plugin.operators.salesforce_to_file_operator import (
            SalesforceToFileOperator,
        )

        operator_instance = SalesforceToFileOperator(
            dag=None,
            task_id="test_salesforce_to_file_operator",
            conn_id="salesforce_default",
            soql=sql_file.sql_text,
            soql_args=soql_params,
            filepath=target.strpath,
        )
        operator = Operator(operator=operator_instance, target=target.strpath)
        yield operator
