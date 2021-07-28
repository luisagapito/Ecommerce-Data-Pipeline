from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,  LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import datetime

# Script with the SQL statements used for this pipeline
import sql_statements


# DAG uses this default_args dict
default_args = {
    'owner': 'luis_agapito',
    'start_date': datetime.datetime.utcnow(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup_by_default': False,
    'max_active_runs': 1
}


# Dag run once an hour and the DAG object has default args set
dag = DAG('corp_analytics_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )


# DAG begins with a start_execution task
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


# Tasks for the stage tables

# Task for the Stage_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",   
    s3_bucket="corp-landing-analytics",
    s3_key="Events/2019-Dec.csv",
    copy_params = "IGNOREHEADER 1 delimiter ',' csv"
)


# Task for the Stage_customer table
stage_customer_to_redshift = StageToRedshiftOperator(
    task_id="Stage_customer",
    dag=dag,
    table="staging_customer",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",   
    s3_bucket="corp-landing-analytics",
    s3_key="Customer/",
    copy_params = "IGNOREHEADER 1 delimiter ',' csv"
)


# Task for the Stage_products table
stage_products_to_redshift = StageToRedshiftOperator(
    task_id="Stage_products",
    dag=dag,
    table="staging_products",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",   
    s3_bucket="corp-landing-analytics",
    s3_key="Products/",
    copy_params ="TRUNCATECOLUMNS json 'auto ignorecase'"
)


# Task for the fact table
load_product_sales_table = LoadFactOperator(
    task_id='Load_product_sales_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="product_sales",
    staging_events="staging_events",
    staging_customer="staging_customer",
    staging_products="staging_products"
)

# Tasks for the dimensional tables


# Task for the users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="users",
    insert_sql=sql_statements.user_table_insert,
    append_data=False
)

# Task for the time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="time",
    insert_sql=sql_statements.time_table_insert,
    append_data=True
)



# Task for the songs table
load_product_dimension_table = LoadDimensionOperator(
    task_id='Load_product_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dim_table="products",
    insert_sql=sql_statements.products_table_insert,
    append_data=False
)


# Task for the quality tests
# The quality operator accepts a list of dict objects with both the SQL queries to check and their expected results
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'test_sql': "SELECT COUNT(*) FROM products WHERE product_id is null", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM time WHERE event_time is null", 'expected_result': 0},
        {'test_sql': "SELECT COUNT(*) FROM users WHERE customer_id is null", 'expected_result': 0},
        {'test_sql': "select count(distinct product_brand) from product_sales", 'expected_result': 151},
        {'test_sql': "select count(distinct gender) from users", 'expected_result': 2},
        {'test_sql': "select distinct event_type from product_sales", 'expected_result': 'purchase'}
    ]
)
# DAG ends with a end_execution task.
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



# Task ordering
start_operator >> stage_events_to_redshift
start_operator >> stage_customer_to_redshift
start_operator >> stage_products_to_redshift
stage_products_to_redshift >> load_product_sales_table
stage_events_to_redshift >> load_product_sales_table
stage_customer_to_redshift >> load_product_sales_table
load_product_sales_table >> load_time_dimension_table
load_product_sales_table >> load_user_dimension_table
load_product_sales_table >> load_product_dimension_table
load_time_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_product_dimension_table >> run_quality_checks
run_quality_checks >> end_operator