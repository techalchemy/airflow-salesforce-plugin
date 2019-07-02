# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>

import inspect
import sys
from typing import List

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.log.logging_mixin import LoggingMixin

from .hooks import SalesforceHook
from .operators import (
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
