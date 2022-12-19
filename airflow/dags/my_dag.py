from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from open_meto_api import OpenMetoClient


# pxPkF83QxszbTnVk
def _get_forecast_for_random_loc():
    client = OpenMetoClient()
    return client.get_default_forecast_for_location(50.07, 13.41)


with DAG('get_forecast',
         start_date=datetime(2022, 12, 19),
         schedule_interval='* * * * *',
         catchup=False) as dag:
    response = PythonOperator(
        task_id='my_dag',
        python_callable=_get_forecast_for_random_loc
    )


