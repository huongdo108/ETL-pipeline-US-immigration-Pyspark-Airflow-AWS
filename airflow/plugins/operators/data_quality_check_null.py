import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class NullRecordCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id="", pairs_table_column=[], *args, **kwargs):

        super(NullRecordCheckOperator, self).__init__(*args, **kwargs)
        self.pairs_table_column = pairs_table_column
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for pair in self.pairs_table_column:
            table = pair[0]
            column = pair[1]
            records = redshift_hook.get_records(f"SELECT 1 FROM {table} where {table}.{column} is NULL limit 1")
            if len(records) > 0:
                raise ValueError(
                    f"SELECT COUNT(*) FROM {table} where {table}.{column} is NULL: Data quality check failed. There are null records"
                )

            logging.info(f"{table}: Data quality check passed with 0 null records")
