from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tests=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        logging.info("Starting data quality checks")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in self.tests:
            sql = test.get("sql")
            expected = test.get("expected_result")
            logging.info(f"Running test: {sql}")
            records = redshift.get_records(sql)
            if not records or not records[0]:
                raise ValueError(
                    f"Data quality check failed. Query returned no results --> {sql}"
                )
            result = records[0][0]
            if result != expected:
                raise ValueError(
                    f"Data quality check failed. Query --> {sql} "
                    f"Expected: {expected}, Got --> {result}"
                )
            logging.info(f"Data quality check passed. Result --> {result}")