from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import clean_and_convert_movies_data 
import clean_and_convert_series_data
import insert_data_into_redshift
import insert_data_into_s3

with DAG("Create_data_warehouse_dag", start_date=datetime(2023, 2, 17), schedule_interval="25 13 * * *", catchup=False) as dag:
    with TaskGroup("process_movies_data") as process_movies_data:
        clean_movies_data = PythonOperator(
            task_id = "clean_movies_data",
            python_callable = clean_and_convert_movies_data.clean_data
        )

        merge_movies_data = PythonOperator(
            task_id = "merge_movies_data",
            python_callable = clean_and_convert_movies_data.merge_data
        )

        insert_movies_data_into_s3 = PythonOperator(
            task_id = "insert_data_into_s3",
            python_callable = insert_data_into_s3.insert_movies_data
        )

        insert_movies_data_into_redshift = PythonOperator(
            task_id = "insert_data_into_redshift",
            python_callable = insert_data_into_redshift.insert_movies_data
        )
        clean_movies_data >> merge_movies_data >> insert_movies_data_into_s3 >> insert_movies_data_into_redshift
    
    with TaskGroup("process_series_data") as process_series_data:
        clean_series_data = PythonOperator(
            task_id = "clean_series_data",
            python_callable= clean_and_convert_series_data.clean_data
        )

        merge_series_data = PythonOperator(
            task_id = "merge_series_data",
            python_callable = clean_and_convert_series_data.merge_data
        )

        insert_series_data_into_s3 = PythonOperator(
            task_id = "insert_data_into_s3",
            python_callable = insert_data_into_s3.insert_series_data
        )

        insert_series_data_into_redshift = PythonOperator(
            task_id = "insert_data_into_redshift",
            python_callable = insert_data_into_redshift.insert_series_data
        )
        clean_series_data >> merge_series_data >> insert_series_data_into_s3 >> insert_series_data_into_redshift

    [process_movies_data, process_series_data]
