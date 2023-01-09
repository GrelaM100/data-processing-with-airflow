from typing import List
from datetime import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from azure.storage.blob import ContainerClient
import pandas as pd

from open_meteo.open_meto_api import OpenMetoClient
from geoapi import GeoApiClient

logger = logging.getLogger(__name__)


def _get_container_client() -> ContainerClient:
    azure_blob_connection_string: str = Variable.get(
        "azure_blob_connection_string"
    ) 
    azure_blob_container_name: str = Variable.get(
        "azure_blob_container_name"
    )

    return ContainerClient.from_connection_string(
        conn_str=azure_blob_connection_string,
        container_name=azure_blob_container_name
    )


def _get_forecast_for_random_loc():
    import random
    client = OpenMetoClient()
    longitude = round(random.uniform(-180, 180), 2)
    latitude = round(random.uniform(-90, 90), 2)
    data = client.get_default_forecast_for_location(longitude, latitude)

    return data


def _transform_data(ti):
    locations_data = ti.xcom_pull(task_ids=[
        'download_data_1',
        'download_data_2',
        'download_data_3'
    ])

    df: pd.DataFrame = pd.DataFrame([])

    logger.info(f"Fetching data for {len(locations_data)} locations.")
    for location_data in locations_data:
        latitude: float = location_data['latitude']
        longitude: float = location_data['longitude']
        datetime: List[float] = location_data['hourly']['time']
        temperature: List[float] = location_data['hourly']['temperature_2m']

        df_location_specific = pd.DataFrame({
            'temperature': temperature,
            'datetime': datetime
        })

        df_location_specific.insert(0, 'longitude', longitude)
        df_location_specific.insert(0, 'latitude', latitude)

        logger.info(f"Fetched data for location: ({latitude}, {longitude})")
        df = pd.concat([df, df_location_specific], axis=0)
    
    _upload_dataframe_to_blob(df, filename="raw")


def _upload_dataframe_to_blob(df: pd.DataFrame, filename: str) -> None:
    container_client = _get_container_client()
    df.to_parquet(f"./{filename}.parquet")
    blob_client = container_client.get_blob_client(f"{filename}.parquet")
    blob_client.upload_blob(f"./{filename}.parquet", overwrite=True)


def _extract_geoapi_data(extractor_id: str) -> pd.DataFrame:
    logger.info(f"Initializing GeoAPI extractor with ID {extractor_id}")
    offset_mapping = {'1': 0, '2':700, '3':1400, '4':2100}
    geoapi_client = GeoApiClient(
        country="Poland",
        offset=offset_mapping[extractor_id]
    )
    df: pd.DataFrame = geoapi_client.extract()
    print(df.head(20))
  
    _upload_dataframe_to_blob(df, filename=f"geoapi_extractor_{extractor_id}")

    return df.to_json(orient="records")


def _transform_geoapi_data(ti) -> pd.DataFrame:
    geoapi_data = ti.xcom_pull(task_ids=[
        'geoapi_extractor_1',
        'geoapi_extractor_2',
        'geoapi_extractor_3',
        'geoapi_extractor_4'
    ])

    for data in geoapi_data:
        print(type(data))


with DAG(
        'get_forecast',
        start_date=datetime(2022, 12, 19),
        schedule_interval='* * * * *',
        catchup=False
    ) as dag:


    with TaskGroup(group_id="geoapi_extract_transform_load") as geoapi:
        extractors = [
            PythonOperator(
                task_id=f"geoapi_extractor_{extractor_id}",
                python_callable=_extract_geoapi_data,
                op_kwargs={
                    'extractor_id': extractor_id
                }
            ) for extractor_id in ['1', '2', '3', '4']
        ]

        transformer = PythonOperator(
            task_id=f"geoapi_transformer",
            python_callable=_transform_geoapi_data,
        )

        extractors >> transformer

    responses = [
        PythonOperator(
            task_id=f'download_data_{data_id}',
            python_callable=_get_forecast_for_random_loc,
            op_kwargs={'data': data_id}
        ) for data_id in ['1', '2', '3']
    ]

    choosing_hottest_loc = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_data
    )

    geoapi
    geoapi >> responses >> choosing_hottest_loc
