# Copyright (c) 2019 Dan Ryan
# MIT License <https://opensource.org/licenses/mit>

import codecs
import configparser
import pathlib

import setuptools

BASE_DIR = pathlib.Path(__file__).parent


setuptools.setup(
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    package_data={"": ["LICENSE*", "README*"]},
    entry_points={
        "airflow.plugins": [
            "salesforce_plugin = airflow_salesforce_plugin.plugin:SalesforcePlugin"
        ]
    },
)
