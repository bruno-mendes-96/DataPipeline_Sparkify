from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """  The Operator receives the following variales:
    redshift connection, table name, an insert sql query statement.
    Ideally, this operator doesn't truncate the target table before 
    executing the insert declaration (append only).

    Args:
        BaseOperator: Base Operator of Airflow
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_statement="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.insert_statement = insert_statement
        self.redshift_conn_id = redshift_conn_id
        self.truncate=truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Inserting data into table")
        insert_statement_completed = f"INSERT INTO {self.table} \n" \
            + self.insert_statement

        redshift.run(insert_statement_completed)
