Airflow Salesforce Plugin
=========================

Provide connection and data movement operations from Salesforce.

Hooks
-----

Available hooks within this plugin are the following...

``SalesforceHook``
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    salesforce_hook = SalesforceHook(salesforce_connection_id)

    # print all rows that are results of runing soql
    for row in salesforce_hook.query(soql, [parameters], include_headers=True):
        print(row)

    # export results to filepath as a csv
    export_filepath = salesforce_hook.export(soql, filepath, [parameters], include_headers=True)


Operators
---------

Available operators within this plugin are the following...

``SalesforceToFileOperator``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # export results from running sql to a given filepath
    salesforce_to_file = SalesforceToFileOperator(
        task_id="salesforce_to_file",
        salesforce_conn_id=salesforce_connection_id,
        soql=soql,
        soql_args="arg1,arg2,arg3",
        filepath=filepath
    )
