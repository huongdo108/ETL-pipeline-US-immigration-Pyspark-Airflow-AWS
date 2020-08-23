import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class ExistRecordCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id="", tables=[], *args, **kwargs):

        super(ExistRecordCheckOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"{table}: Data quality check failed. Returned no records")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"{table}: Data quality check failed. Contained 0 rows")
            logging.info(f"{table}: Data quality check passed with {records[0][0]} records")
