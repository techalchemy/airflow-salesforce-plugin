# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>

from __future__ import absolute_import, unicode_literals

import collections
import csv
import io
import pathlib
import sqlite3
import tempfile
import uuid
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union

import pandas as pd
import sqlparse
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from simple_salesforce.api import Salesforce, exception_handler


class StreamingSalesforce(Salesforce):
    def query_streaming(self, query, include_deleted=False, **kwargs):
        url = self.base_url + ("queryAll/" if include_deleted else "query/")
        params = {"q": query}
        # `requests` will correctly encode the query string passed as `params`
        return self._call_salesforce(
            "GET", url, name="query", params=params, stream=True, **kwargs
        )

    def query_more_streaming(
        self,
        next_records_identifier,
        identifier_is_url=False,
        include_deleted=False,
        **kwargs,
    ):
        if identifier_is_url:
            # Don't use `self.base_url` here because the full URI is provided
            url = f"https://{self.sf_instance}{next_records_identifier}"
        else:
            endpoint = "queryAll" if include_deleted else "query"
            url = f"{self.base_url}{endpoint}/{next_records_identifier}"
        return self._call_salesforce("GET", url, name="query_more", stream=True, **kwargs)


class SalesforceHook(BaseHook):

    """
    Salesforce connection hook.

    ..note:: Uses `simple_salesforce <https://github.com/simple-salesforce/>`_
        with `requests <https://github.com/requests/requests>`_ for all connections
        and queries to Salesforce.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "salesforce_default"

    def __init__(self, conn_id: str = "salesforce_default", *args, **kwargs) -> None:
        self.conn_id: str = conn_id
        self._args = args
        self._kwargs = kwargs
        self.connection = self.get_connection(self.conn_id)
        self.session: Optional[StreamingSalesforce] = None

    def login(self) -> "SalesforceHook":
        """
        Establishes a connection to Salesforce.

        :return: The current object instance
        :rtype: SalesforceHook
        """

        if isinstance(self.session, StreamingSalesforce):
            return self

        self.log.info(f"establishing connection to salesforce {self.connection.host!r}")
        extras: Dict[str, str] = self.connection.extra_dejson
        auth_type: str = extras.get("auth_type", "password")
        auth_kwargs: Dict[str, Union[Optional[str], bool]] = {}
        sandbox = extras.get("sandbox", False) in ("yes", "y", "1", 1, True)
        if auth_type == "direct":
            auth_kwargs = {
                "instance_url": self.connection.host,
                "session_id": self.connection.password,
            }
        else:
            auth_kwargs = {
                "username": self.connection.login,
                "password": self.connection.password,
                "security_token": extras.get("security_token"),
                # "instance_url": self.connection.host,
                "sandbox": sandbox,
            }
        self.log.info("Signing into salesforce...")
        self.log.info(f"Using username: {self.connection.login} with sandbox={sandbox!s}")
        self.session = StreamingSalesforce(**auth_kwargs)  # type: ignore
        if not self.session:
            self.log.error(f"Failed connecting to salesforce: {self.session}")
            raise AirflowException("Connection failure during salesforce connection!")
        return self

    def _parse_identifiers(self, sql: str) -> Tuple[str, List[str]]:
        """
        Parses a sql statement for the first identifier list.

        :param sql: The sql statement to parse
        :type sql: str
        :return: A tuple of the table name and the list of identifiers
        :rtype: Tuple[str, List[str]]
        """

        table_name: str = ""
        columns: List[str] = []
        statements: List[Any] = sqlparse.parse(sql)

        # NOTE: only parses first full statement
        for token in statements[0].tokens:
            if isinstance(token, (sqlparse.sql.IdentifierList,)):
                for identifier in token:
                    if (
                        isinstance(identifier, (sqlparse.sql.Identifier,))
                        or identifier.ttype in (sqlparse.tokens.Keyword,)
                        or identifier.value.lower() in ("type", "alias")
                    ):
                        columns.append(identifier.value)
            elif isinstance(token, (sqlparse.sql.Identifier,)) or token.ttype in (
                sqlparse.tokens.Keyword,
            ):
                table_name = token.value

        return (table_name, columns)

    def _extract_columns(self, sql: str, replace: Dict[str, str] = None) -> List[str]:
        """
        Extracts column names from a given sql statement.

        :param sql: The sql statement to parse
        :type sql: str
        :param replace: Character replacement for the column names, defaults to None
        :param replace: Dict, optional
        :return: The extracted column names
        :rtype: List[str]
        """

        if replace is None:
            replace = {}
        columns: List[str] = []
        for field in self._parse_identifiers(sql)[-1]:
            for (old, new) in replace.items():
                field = field.replace(old, new)
            columns.append(field)
        return columns

    def _extract_row(self, result: collections.OrderedDict) -> List[Any]:
        """
        Extracts the rows from a given result from querying Salesforce.

        :param result: The result to extract rows from
        :type result: collections.OrderedDict
        :return: A list of values
        :rtype: list
        """

        values: List[Any] = []
        for (column, value) in result.items():
            if column.lower() not in ("attributes",):
                if isinstance(value, collections.OrderedDict):
                    values.extend(self._extract_row(value))
                else:
                    values.append(value)
        return values

    def _get_results(self, sql: str, url: str = None, gen_uuid: str = None, **kwargs):
        """
        Query the session instance for a response using the supplied sql or next url

        :param str sql: Sql to execute
        :param Optional[str] url: Url to visit, (default: None)
        :param Optional[str] gen_uuid: UUID for the session, (default: None)
        :return: A response instance from the session
        :rtype: :class:`requests.PreparedResponse`
        """
        assert self.session
        if not url:
            kwargs.update({"include_deleted": True})
            return self.session.query_streaming(sql, **kwargs)
        return self.session.query_more_streaming(url, identifier_is_url=True)

    def _gen_result_frames(
        self,
        sql: str,
        columns: List[str],
        page: int = 1,
        url: str = None,
        gen_uuid: str = None,
        include_headers: bool = False,
        **kwargs,
    ) -> Optional[pd.DataFrame]:
        """
        Generates results from executing a sql statement.

        :param str sql: The sql statement to execute
        :param Optional[str] url: The url of the next page of results (default: None)
        :param Optional[str] gen_uuid: The session-specific UUID (default: None)
        :return: A generator over the :class:`pd.DataFrame` instances from the results
        :rtype: Generator
        """

        if not gen_uuid:
            gen_uuid = str(uuid.uuid4())
        try:
            df = pd.read_json(
                self._get_results(sql, url=url, gen_uuid=gen_uuid).iter_lines(),
                lines=True,
                chunk=1,
            )
        except Exception:
            df = pd.read_json(self._get_results(sql, url=url, gen_uuid=gen_uuid).text)
        new_df = pd.DataFrame.from_records(
            df.records.apply(lambda x: {k: v for k, v in x.items() if k != "attributes"})
        )
        df["page"] = page
        if not df["records"].count():
            return new_df
        df["page"] = page
        unique_records = df.done.unique()
        try:
            done = unique_records[0]
        except IndexError:
            done = True
        if new_df.columns.size < 1:
            new_df.columns = [c.replace(".", "_") for c in columns]
        else:
            new_df.columns = new_df.columns.str.lower()
            joined_tables: Dict[str, Set[str]] = collections.defaultdict(set)
            join_columns = [col for col in columns if "." in col]
            for col in join_columns:
                table, _, col_name = col.partition(".")
                joined_tables[table].add(col_name)
            for table in joined_tables:
                table_df = new_df[table].apply(pd.Series)
                table_df = table_df.drop("attributes", axis=1).add_prefix(f"{table}_")
                table_df.columns = table_df.columns.str.lower()
                new_df = pd.concat([new_df.drop([table], axis=1), table_df], axis=1)
        columns = [c.replace(".", "_").lower() for c in columns]
        new_df = new_df[columns]
        yield new_df
        next_url = None
        if not done:
            page += 1
            next_url = df.nextRecordsUrl.unique()[0]
            for next_df in self._gen_result_frames(
                sql,
                columns=columns,
                url=next_url,
                gen_uuid=gen_uuid,
                include_headers=False,
            ):
                yield next_df

    def dump_sqlite(
        self, sql: str, filepath: str, table: str, parameters: List[str], **kwargs
    ):
        """Dump the results of the supplied sql to the target sqlite database

        :param str sql: Parameterized soql to use for querying
        :param str filepath: Filepath to export results to
        :param str table: The target table
        :param List[str] parameters: Parameters to use as arguments to the soql
        :return: A count of the rows written
        :rtype: int
        """
        if not self.session:
            self.login()
        if parameters is None:
            parameters = []
        if len(parameters) > 0:
            sql = sql % tuple(parameters)
        kwargs.update({"include_deleted": True})
        columns = [col for col in self._extract_columns(sql)]
        filepath = pathlib.Path(filepath).absolute().as_posix()
        sqlite_conn = sqlite3.connect(filepath)
        rows = 0
        include_headers = True
        for df in self._gen_result_frames(
            sql, columns=columns, include_headers=include_headers, **kwargs
        ):
            rows += len(df)
            df.to_sql(
                table, con=sqlite_conn, if_exists="append", chunksize=200, index=False
            )
        return rows

    def dump_csv(
        self,
        sql: str,
        filepath: str,
        parameters: List[str],
        include_headers: bool = True,
        **kwargs,
    ) -> int:
        """Dump the results of the requested sql to CSV

        :param str sql: Parameterized soql to use for querying
        :param str filepath: Filepath to export results to
        :param List[str] parameters: Parameters to use as arguments to the soql
        :param bool include_headers: Whether to include headers in the output, defaults to True
        :return: A count of the rows written
        :rtype: int
        """
        if not self.session:
            self.login()
        if parameters is None:
            parameters = []
        if len(parameters) > 0:
            sql = sql % tuple(parameters)
        kwargs.update({"include_deleted": True})
        columns = [col for col in self._extract_columns(sql)]
        temp_dir = tempfile.TemporaryDirectory(prefix="salesforce", suffix="csvs")
        if not filepath:
            tmpfile = tempfile.NamedTemporaryFile(
                prefix="airflow",
                suffix="salesforce.csv",
                delete=False,
                dir=temp_dir.name,
                encoding="utf-8",
            )
            filepath = tmpfile.name
            tmpfile.close()
        rows = 0
        for df in self._gen_result_frames(
            sql,
            columns=columns,
            temp_dir=temp_dir,
            temp_file=filepath,
            include_headers=include_headers,
            **kwargs,
        ):
            rows += len(df)
            df.to_csv(
                filepath,
                chunksize=200,
                header=include_headers,
                encoding="utf-8",
                index=False,
                mode="a",
            )
            if include_headers:
                include_headers = False
        return rows

    def _gen_results(
        self, sql: str, url: str = None, gen_uuid: str = None, **kwargs
    ) -> Generator[List[Any], None, None]:
        """
        Generates results from executing a sql statement.

        :param str sql: The sql statement to execute
        :param Optional[str] url: The url of the next page of results (default: None)
        :param Optional[str] gen_uuid: The session-specific UUID (default: None)
        :return: A generator which resulting rows from executing the given sql
        :rtype: Generator
        """
        # create generation uuid to show what logs are part of the same process
        if not gen_uuid:
            gen_uuid = str(uuid.uuid4())
        results = self._get_results(sql, url=url, gen_uuid=gen_uuid)
        if not results:
            yield []

        next_url = None
        for row in results.get("records", []):
            yield self._extract_row(row)
        if not results["done"]:
            next_url = results.get("nextRecordsUrl", None)
            for row in self._gen_results(sql, url=next_url, gen_uuid=gen_uuid):
                yield row

    def query(
        self,
        sql: str,
        parameters: List[str] = None,
        include_headers: bool = True,
        **kwargs,
    ) -> Generator[List[Any], None, None]:
        """
        Generates the results from executing a given sql statement

        :param sql: The given sql statement to execute
        :type sql: str
        :param parameters: The parameters to use for the sql, defaults to None
        :type parameters: List[str], optional
        :param include_headers: True to include columns as the first result,
            defaults to True
        :type include_headers: bool, optional
        :return: A generator which iterates over results from executing the givne sql
        :rtype: Generator
        """

        if parameters is None:
            parameters = []
        if len(parameters) > 0:
            sql = sql % tuple(parameters)

        kwargs.update({"include_deleted": True})
        self.log.info(f"executing query {sql!r}")

        if include_headers:
            yield self._extract_columns(sql, replace={".": "_"})

        self.login()

        for row in self._gen_results(sql, **kwargs):
            yield row

    def export(
        self,
        sql: str,
        filepath: str,
        parameters: List[str] = None,
        include_headers: bool = True,
        **kwargs,
    ) -> str:
        """
        Exports the result of executing sql to a given filepath.

        :param sql: The sql statement to execute
        :type sql: str
        :param filepath: The filepath to export results to
        :type filepath: str
        :param parameters: The parameters to use for the sql, defaults to None
        :type parameters: List[str], optional
        :param include_headers: True to include columns as the first result,
            defaults to True
        :type include_headers: bool, optional
        :return: The resulting exported filepath
        :rtype: str
        """

        self.log.info(f"writing results of sql {sql!r} to {filepath!r}")
        if not parameters:
            parameters = []
        rowcount = self.dump_csv(
            sql=sql, filepath=filepath, parameters=parameters, include_headers=True
        )
        self.log.info(f"*** TASK COMPLETE ***: Wrote {rowcount!s} rows!")
        return filepath
