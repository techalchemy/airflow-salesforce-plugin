from typing import List

from airflow.plugins_manager import AirflowPlugin

from airflow_salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from airflow_salesforce_plugin.operators import (
    SalesforceAttachmentToS3Operator,
    SalesforceToFileOperator,
    SalesforceToS3Operator,
)


class SalesforcePlugin(AirflowPlugin):
    """Apache Airflow Salesforce Plugin."""

    name = "salesforce_plugin"
    hooks: List = [SalesforceHook]
    operators: List = [
        SalesforceToFileOperator,
        SalesforceAttachmentToS3Operator,
        SalesforceToS3Operator,
    ]
    executors: List = []
    macros: List = []
    admin_views: List = []
    flask_blueprints: List = []
    menu_links: List = []


__all__ = ["SalesforcePlugin"]
