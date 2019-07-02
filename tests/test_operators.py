from datetime import datetime
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.models import TaskInstance

# from airflow_salesforce_plugin.operators import SalesforceAttachmentToS3Operator, SalesforceToFileOperator, SalesforceToS3Operator


def test_salesforce_to_file_operator(
    soql_params, csv_dir, sql_file, salesforce_to_file_operator
):
    csv_filename = f"{sql_file.name}.csv"
    operator = salesforce_to_file_operator.operator
    target = salesforce_to_file_operator.target
    operator.hook = MagicMock()
    # with patch("airflow_salesforce_plugin.hooks.salesforce_hook.SalesforceHook") as mock_hook:
    soql_params = ",".join(soql_params)
    operator.execute(context={})
    # operator.hook.assert_called_once_with(conn_id=operator.conn_id)
    operator.hook.export.assert_called_once_with(
        sql_file.sql_text, target, soql_params.split(","), True
    )
