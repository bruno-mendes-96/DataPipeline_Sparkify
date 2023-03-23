from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ DataQualityOpertor receives all tables from star schema and
    the redshift connection.
        The Data Quality Operator checks the number of rows of
    all tables and raises a Value Error in case of empty table. 

    Args:
        BaseOperator: Airflow Base Operator

    Raises:
        ValueError: No instances into table
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_list=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table_name in self.tables_list:
            self.log.info(f"Checking records from {table_name}!")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name}")  
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table_name} returned no results")
                raise ValueError(f"Data quality check failed. {table_name} returned no results")
                
            num_records = records[0][0]
            if num_records == 0:
                self.log.error(f"No records present in destination table {table_name}")
                raise ValueError(f"No records present in destination {table_name}")
                
            self.log.info(f"Data quality on table {table_name} check passed with {num_records} records")