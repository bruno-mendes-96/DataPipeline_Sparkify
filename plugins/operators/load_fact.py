from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id
        self.truncate=truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Inserting data into table")
        insert_statement = f"INSERT INTO {self.table} \n" \
            + self.sql_query

        redshift.run(insert_statement)