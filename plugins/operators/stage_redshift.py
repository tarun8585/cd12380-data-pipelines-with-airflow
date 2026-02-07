from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        FORMAT AS JSON '{json_format}'
        REGION 'us-west-2';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        self.log.info(f"Staging {self.table} from S3 to Redshift")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws = BaseHook.get_connection(self.aws_credentials_id)

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=aws.login,
            secret_key=aws.password,
            json_format=self.json_format
        )

        self.log.info(f"Running COPY command for table --->  {self.table}")
        redshift.run(formatted_sql)
        self.log.info(f"Finished staging table --->  {self.table}")