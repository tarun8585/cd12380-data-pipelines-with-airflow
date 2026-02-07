from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        json_format="auto",
        region="us-west-2",
        *args,
        **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.region = region

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        logging.info(f"Clearing data from Redshift staging table --> {self.table}")
        redshift.run(f"DELETE FROM {self.table}")
        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            IAM_ROLE '{{}}'
            FORMAT AS JSON '{self.json_format}'
            REGION '{self.region}';
        """
        aws_hook = PostgresHook(postgres_conn_id=self.aws_credentials_id)
        aws_hook = PostgresHook(postgres_conn_id=self.aws_credentials_id)
        iam_role = aws_hook.get_connection(self.aws_credentials_id).extra_dejson.get("role_arn")
        formatted_sql = copy_sql.format(iam_role)
        logging.info(f"Running COPY command for table --> {self.table}")
        logging.info(f"COPY SQL:\n{formatted_sql}")
        redshift.run(formatted_sql)
        logging.info(f"Stage load completed for table --> {self.table}")