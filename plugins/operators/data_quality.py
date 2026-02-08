from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    template_fields = ("tables",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables or []

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Running data quality check on table: {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table};")
            # Check that we got a result at all
            if not records or not records[0] or records[0][0] is None:
                error_msg = f"Data quality check failed. No results returned for table {table}."
                self.log.error(error_msg)
                raise AirflowException(error_msg)
            count = records[0][0]
            # Check that the table is not empty
            if count == 0:
                error_msg = f"Data quality check failed. Table {table} returned 0 rows."
                self.log.error(error_msg)
                raise AirflowException(error_msg)
            self.log.info(f"Data quality check passed for table {table}. Row count: {count}")