from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    copy_sql = """
    INSERT INTO {}
        (event_time,product_id,product_name,product_brand,product_price,customer_id,customer_payment_method,customer_total_charges,customer_contract)
        SELECT
                events.event_time,
                events.product_id,
                products.name,
                events.brand,
                cast(events.price as float) as product_price,
                customers.user_id,
                customers.paymentmethod,
                cast(customers.totalcharges as float) as customer_total_charges,
                customers.contract
                FROM (SELECT distinct cast(event_time as timestamp) event_time, product_id, brand, price,user_id
            FROM {}
            WHERE event_type = 'purchase') events
            LEFT JOIN {} customers
            ON events.user_id = customers.user_id
            left join {} products
            on events.product_id = products.product_id
            where customers.user_id IS NOT NULL
    """


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 staging_events="",
                 staging_customer="",
                 staging_products="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.staging_events = staging_events
        self.staging_customer = staging_customer
        self.staging_products = staging_products


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #For Fact table only inserts data as suggessted in the project
        self.log.info("Inserting data to fact table")
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.fact_table,
            self.staging_events,
            self.staging_customer,
            self.staging_products
        )
        redshift.run(formatted_sql)
        
        
        
        
        
        
