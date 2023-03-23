from importlib_metadata import metadata
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """
    
    copy_sql_partitioned = """
        COPY {}
        FROM '{}/{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 metadata_path="",
                 delimiter=",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.execution_date = kwargs.get('execution_date')
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_path = s3_path
        self.delimiter = delimiter
        self.aws_credentials_id = aws_credentials_id
        self.metadata_path = metadata_path
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        if self.execution_date:
            formatted_sql = StageToRedshiftOperator.copy_sql_partitioned.format(
                self.table, 
                self.s3_path, 
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key,
                self.metadata_path
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key,
                self.metadata_path
            )
        redshift.run(formatted_sql)

