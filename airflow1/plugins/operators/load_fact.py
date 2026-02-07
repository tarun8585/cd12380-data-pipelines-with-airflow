from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql_statement="",
        *args,
        **kwargs
    ):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):
        logging.info(f"Starting fact table load for --> {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
        """
        logging.info(f"Executing SQL for fact load:\n{insert_sql}")
        redshift.run(insert_sql)
        logging.info(f"Fact table load completed for: {self.table}")