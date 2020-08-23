import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.immigration_plugin import ExistRecordCheckOperator, NullRecordCheckOperator, RunAnalyticsOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from helpers import ImmigrationAnalytics

US_immigration_schema = "production"

default_args = {
    "owner": "US_immigration",
    "start_date": "2020-08-22T13:14:12.913287+00:00",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False,
}


dag = DAG(
    "us_immigration",
    description="Execute ETL job in main.py",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
)

# create a starting operator
start_operator = DummyOperator(task_id="Begin_execution", dag=dag)

# create connection from Airflow to emr
sshHook = SSHHook(ssh_conn_id="ssh_default")

# zip all necessary python code to send to EMR
zip_executables = BashOperator(
    task_id="prepare_executable_files",
    bash_command="zip -r /home/workspace/executables.zip /home/workspace/awsc/ /home/workspace/config.cfg /home/workspace/main.py",
    dag=dag,
)

# # Copy files from Docker container folder to the EMR Spark directory
deploy_job = SFTPOperator(
    task_id="deploy",
    ssh_hook=sshHook,
    local_filepath="/home/workspace/executables.zip",
    remote_filepath="/home/hadoop/executables.zip",
    operation="put",
    create_intermediate_dirs=True,
    dag=dag,
    confirm=False,
)

#
ETL_jobs_Operator = SSHOperator(
    task_id="ETLjob",
    command="""
    unzip -o executables.zip &&
    cd home/workspace/ &&
    zip -r awsc.zip awsc  &&
    /usr/bin/spark-submit --py-files awsc.zip --master yarn main.py;
    """,
    ssh_hook=sshHook,
    dag=dag,
)


run_quality_checks_exist_records = ExistRecordCheckOperator(
    task_id="data_quality_check_tables_have_records",
    dag=dag,
    redshift_conn_id="redshift",
    tables=[
        f"{US_immigration_schema}.fact_immigration_events",
        f"{US_immigration_schema}.dim_countries",
        f"{US_immigration_schema}.dim_states",
        f"{US_immigration_schema}.dim_cities_demographics_summary",
        f"{US_immigration_schema}.dim_cities_demographics_race",
        f"{US_immigration_schema}.dim_ports",
        f"{US_immigration_schema}.dim_arrival_modes",
        f"{US_immigration_schema}.dim_visa_intents_types",
    ],
)

run_quality_checks_null_records = NullRecordCheckOperator(
    task_id="data_quality_check_specific_column_has_null_records",
    dag=dag,
    redshift_conn_id="redshift",
    pairs_table_column=[
        (f"{US_immigration_schema}.fact_immigration_events", "immigration_id"),
        (f"{US_immigration_schema}.dim_countries", "country_code"),
        (f"{US_immigration_schema}.dim_states", "state_name"),
        (f"{US_immigration_schema}.dim_cities_demographics_summary", "state_code"),
        (f"{US_immigration_schema}.dim_cities_demographics_race", "state_code"),
        (f"{US_immigration_schema}.dim_ports", "port_city"),
        (f"{US_immigration_schema}.dim_arrival_modes", "arrival_mode_type"),
        (f"{US_immigration_schema}.dim_visa_intents_types", "visa_intents_type"),
    ],
)

run_analytics_queries = RunAnalyticsOperator(
    task_id="analytics_queries",
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=[
        ImmigrationAnalytics.query_top_origin_countries(),
        ImmigrationAnalytics.query_top_destination_cities(),
        ImmigrationAnalytics.query_reason_visit_by_country(),
    ],
)

end_operator = DummyOperator(task_id="Stop_execution", dag=dag)

start_operator >> zip_executables >> deploy_job >> ETL_jobs_Operator >> run_quality_checks_exist_records >> run_quality_checks_null_records >> run_analytics_queries >> end_operator


