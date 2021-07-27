from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks


    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []
        
        self.log.info('The quality operator compares the SQL queries with their expected results')
        
        for check in self.dq_checks:
            
            #SQL queries
            sql = check.get('test_sql')
            #Expected Results
            exp_result = check.get('expected_result')
            records = redshift.get_records(sql)[0]
            
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
                
        if error_count > 0:
            self.log.info('Tests have failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality checks have failed')
            
        self.log.info('Data Quality checks successfully passed!!')