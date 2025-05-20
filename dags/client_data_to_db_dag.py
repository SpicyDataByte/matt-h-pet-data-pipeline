from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_xlsx
from etl.transform import *
from etl.validate import *
from etl.load import *

default_args = {'start_date': datetime(2023, 1, 1)}

with DAG('client_data_to_db_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:

## could make these task groups
    t1 = PythonOperator(task_id='extract_xlsx', python_callable=extract_xlsx)
    
    t2 = PythonOperator(task_id='clean_columns', python_callable=clean_columns) 
    t3 = PythonOperator(task_id='convert_column_types', python_callable=convert_column_types)  
    t4 = PythonOperator(task_id='categorize_city', python_callable=categorize_city_column) 
    t5 = PythonOperator(task_id='categorize_province', python_callable=categorize_province_column) 
    t6 = PythonOperator(task_id='standardize_phone', python_callable=standardize_owner_phone_column) 
    t7 = PythonOperator(task_id='infer_province', python_callable=infer_missing_provinces_from_phone) # look at this one
    t8 = PythonOperator(task_id='standardize_unit', python_callable=standardize_unit_column) 
    t9 = PythonOperator(task_id='standardize_species', python_callable=standardize_pet_species_column) 
    # t9 = PythonOperator(task_id='handle_missing', python_callable=handle_missing_values) # Keeping this task out for demo to make solution easier to follow. 
    
    t10 = PythonOperator(task_id='validate_data', python_callable=validate_cleaned_data) 
    t11 = PythonOperator(task_id='deduplicate_data', python_callable=deduplicate_cleaned_data) 
    
    t12 = PythonOperator(task_id='add_surrogate_keys', python_callable=add_surrogate_keys) 
    t13 = PythonOperator(task_id='truncate_table', python_callable=truncate_tables) 
    t14 = PythonOperator(task_id='load_to_postgres', python_callable=load_normalized_tables)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >>t9 >> t10 >> t11 >> t12 >> t13 >> t14
