from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from open_meteo.open_meto_api import OpenMetoClient


# pxPkF83QxszbTnVk
def _get_forecast_for_random_loc():
    import random
    client = OpenMetoClient()
    longitude = round(random.uniform(-180, 180), 2)
    latitude = round(random.uniform(-90, 90), 2)
    data = client.get_default_forecast_for_location(longitude, latitude)

    return data


def _transform_data(ti):
    import pandas as pd
    data = ti.xcom_pull(task_ids=[
        'download_data_1',
        'download_data_2',
        'download_data_3'
    ])
    longitude = []
    latitude = []
    time = []
    temperature = []

    for element in data:
        longitude.append(element['longitude'])
        latitude.append(element['latitude'])
        time.append(element['hourly']['time'][0])
        temperature.append(element['hourly']['temperature_2m'][0])

    data_df = pd.DataFrame({'longitude': longitude,
                            'latitude': latitude,
                            'time': time,
                            'temperature': temperature})

    print(data_df)
    data_df.to_csv('~/PycharmProjects/data-processing-with-airflow/data.csv')
    return data_df.to_json()


with DAG('get_forecast',
         start_date=datetime(2022, 12, 19),
         schedule_interval='* * * * *',
         catchup=False) as dag:
    responses = [PythonOperator(
        task_id=f'download_data_{data_id}',
        python_callable=_get_forecast_for_random_loc,
        op_kwargs={'data': data_id}
    ) for data_id in ['1', '2', '3']]
    choosing_hottest_loc = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_data
    )
    visualize = BashOperator(
        task_id="visualize",
        bash_command="cat ~/PycharmProjects/data-processing-with-airflow/data.csv"
    )

    responses >> choosing_hottest_loc >> visualize
