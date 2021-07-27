from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    sql = """
    INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dim_table="",
                 insert_sql="",
                 append_data="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table
        self.insert_sql = insert_sql
        self.append_data = append_data


    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Depending on the append_data field, it only inserts data or truncates the table before inserting it.
        if self.append_data == True:
            self.log.info("Inserting data to dimensional table")
            formatted_sql = LoadDimensionOperator.sql.format(
                self.dim_table,
                self.insert_sql
            )
            redshift.run(formatted_sql)
        else:
            self.log.info("Clearing data from dimensional table")
            redshift.run("DELETE FROM {}".format(self.dim_table))

            self.log.info("Inserting data to dimensional table")
            formatted_sql = LoadDimensionOperator.sql.format(
                self.dim_table,
                self.insert_sql
            )
            redshift.run(formatted_sql)