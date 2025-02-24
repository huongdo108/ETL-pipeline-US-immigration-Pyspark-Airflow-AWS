from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class RunAnalyticsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, redshift_conn_id="", sql_query=[], *args, **kwargs):
        super(RunAnalyticsOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for query in self.sql_query:
            self.log.info("Running Analytics query :  {}".format(query))
            # redshift_hook.run(str(query))
            records = redshift_hook.get_records(query)
            print(records[0])
            print(records[0][0])
            self.log.info("Query ran successfully!!")
