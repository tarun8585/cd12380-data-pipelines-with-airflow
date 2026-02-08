from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    template_fields = ("sql_statement",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 mode="truncate-insert",   # or "append"
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.mode = mode

    # Added option of truncate-insert if needed, Had to validate the data freshly in rerun scenarios.
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Added fix for feedback
        if self.mode == "truncate-insert":
            self.log.info(f"Clearing data from dimension table --> {self.table}")
            redshift.run(f"DELETE FROM {self.table}")
        # Insert Step.
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_statement};
        """
        self.log.info(f"Loading data into dimension table --> {self.table}")
        self.log.info(f"Running SQL:\n{insert_sql}")
        redshift.run(insert_sql)
        self.log.info(f"Dimension table {self.table} loaded successfully")