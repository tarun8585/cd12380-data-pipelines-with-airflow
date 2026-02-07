from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        table="",
        sql_statement="",
        mode="truncate-insert",   # or "append"
        *args,
        **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.mode = mode

    def execute(self, context):
        logging.info(f"Starting dimension load for --> {self.table}")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == "truncate-insert":
            logging.info(f"Truncating table before insert --> {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement}
        """
        logging.info(f"Executing SQL for dimension load --> \n{insert_sql}")
        redshift.run(insert_sql)
        logging.info(f"Dimension table load completed for --> {self.table}")