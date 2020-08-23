from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class ImmigrationPlugin(AirflowPlugin):
    name = "immigration_plugin"
    operators = [
        operators.ExistRecordCheckOperator,
        operators.NullRecordCheckOperator,
        operators.RunAnalyticsOperator
    ]
