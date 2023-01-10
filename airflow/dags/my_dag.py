import json
from io import BytesIO
from typing import List
from datetime import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
import pandas as pd

from open_meteo.open_meto_api import OpenMetoClient
from geoapi import GeoApiClient

logger = logging.getLogger(__name__)


def _get_blob_service_client(filename: str) -> BlobServiceClient:
    azure_blob_connection_string: str = Variable.get(
        "azure_blob_connection_string"
    )
    azure_blob_container_name: str = Variable.get(
        "azure_blob_container_name"
    )

    blob_service_client = BlobServiceClient.from_connection_string(
        conn_str=azure_blob_connection_string
    )
    blob_client = blob_service_client.get_blob_client(
        container=azure_blob_container_name,
        blob=filename
    )

    return blob_client


def _get_forecast_for_locations(ti):
    client = OpenMetoClient()
    forecast = ti.xcom_pull(task_ids=[
        'fetch_representative_locations'
    ])

    df_representative = pd.read_json(forecast[0], orient='records')

    forecasts = []
    for _, row in df_representative.iterrows():
        forecast = client.get_default_forecast_for_location(
            latitude=row['latitude'],
            longitude=row['longitude'],
        )
        forecasts.append((row['city'], forecast))

    return json.dumps(forecasts)


def _transform_weather_forecasts(ti) -> None:
    weather_forecasts = ti.xcom_pull(task_ids=[
        'run_open_meteo_client'
    ])

    df: pd.DataFrame = pd.DataFrame([])

    weather_forecasts = json.loads(weather_forecasts[0])
    logger.info(f"Transforming forecast data for {len(weather_forecasts)} cities.")
    for idx, city_specific_forecast in enumerate(weather_forecasts, start=1):
        logger.info(f"Running transformation {idx}/{len(weather_forecasts)}")
        city = city_specific_forecast[0]
        location_data = city_specific_forecast[1]

        latitude: float = location_data['latitude']
        longitude: float = location_data['longitude']
        hourly_data = location_data['hourly']
        datetime: List[float] = hourly_data['time']
        temperature: List[float] = hourly_data['temperature_2m']
        apparent_temperature: List[float] = hourly_data['apparent_temperature']
        pressure: List[float] = hourly_data['pressure_msl']
        cloudcover: List[int] = hourly_data['cloudcover']
        windspeed = hourly_data['windspeed_10m']

        df_location_specific = pd.DataFrame({
            'temperature': temperature,
            'apparent_temperature': apparent_temperature,
            'pressure': pressure,
            'cloudcover': cloudcover,
            'windspeed': windspeed,
            'datetime': datetime,
        })

        df_location_specific.insert(0, 'city', city)
        df_location_specific.insert(0, 'longitude', longitude)
        df_location_specific.insert(0, 'latitude', latitude)

        logger.info(f"Transformed data for location: {city} ({latitude}, {longitude})")
        df = pd.concat([df, df_location_specific], axis=0)

    logger.info(f"Finished transformations.")
    logger.info(df.head())
    df.reset_index(drop=True, inplace=True)
    _upload_dataframe_to_blob(df, filename="forecasts")


def _upload_dataframe_to_blob(df: pd.DataFrame, filename: str) -> None:
    blob_service_client = \
        _get_blob_service_client(filename=f"{filename}.parquet")
    parquet_file = BytesIO()
    df.to_parquet(parquet_file, engine='pyarrow')
    parquet_file.seek(0)
    blob_service_client.upload_blob(data=parquet_file, overwrite=True)


def _extract_geoapi_data(extractor_id: str) -> pd.DataFrame:
    logger.info(f"Initializing GeoAPI extractor with ID {extractor_id}")
    offset_mapping = {'1': 0, '2': 700, '3': 1400, '4': 2100}
    geoapi_client = GeoApiClient(
        country="Poland",
        offset=offset_mapping[extractor_id]
    )
    df: pd.DataFrame = geoapi_client.extract()
    print(df.head(20), df.dtypes)

    _upload_dataframe_to_blob(df, filename=f"geoapi_extracted_{extractor_id}")

    return df.to_json(orient='records')


def _transform_geoapi_data(ti) -> pd.DataFrame:
    geoapi_data = ti.xcom_pull(task_ids=[
        'geoapi_extract_transform_load.geoapi_extractor_1',
        'geoapi_extract_transform_load.geoapi_extractor_2',
        'geoapi_extract_transform_load.geoapi_extractor_3',
        'geoapi_extract_transform_load.geoapi_extractor_4'
    ])

    df: pd.DataFrame = pd.DataFrame([])
    df_extractor_data: pd.DataFrame

    for extractor_data in geoapi_data:
        df_extractor_data = pd.read_json(extractor_data, orient='records')
        df = pd.concat([df, df_extractor_data], axis=0)

    df = df.sort_values('population', ascending=False).reset_index(drop=True)
    print(df.head(50))

    _upload_dataframe_to_blob(df, filename="geoapi_transformed")


def _fetch_representative_locations() -> pd.DataFrame:
    filename = "geoapi_transformed.parquet"
    blob_service_client = _get_blob_service_client(filename=filename)
    with BytesIO() as input_blob:
        blob_service_client.download_blob().download_to_stream(input_blob)
        input_blob.seek(0)
        df = pd.read_parquet(input_blob)

    print(df.head(50))
    df_representative = df[:100]
    return df_representative.to_json(orient='records')


with DAG(
        'get_forecast',
        start_date=datetime(2022, 12, 19),
        schedule_interval='0 */3 * * *',
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
            task_id="geoapi_transformer",
            python_callable=_transform_geoapi_data,
        )

        extractors >> transformer

    fetch_representative_locations = PythonOperator(
        task_id="fetch_representative_locations",
        python_callable=_fetch_representative_locations
    )

    run_open_meteo_client = PythonOperator(
        task_id='run_open_meteo_client',
        python_callable=_get_forecast_for_locations
    )

    transform_weather_forecast = PythonOperator(
        task_id='transform_wheater_forecast',
        python_callable=_transform_weather_forecasts
    )

    geoapi >> fetch_representative_locations >> \
        run_open_meteo_client >> transform_weather_forecast
