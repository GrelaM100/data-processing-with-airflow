export AIRFLOW_HOME=~/PycharmProjects/data-processing-with-airflow/airflow
export PYTHONPATH=~/PycharmProjects/data-processing-with-airflow

airflow webserver --port 8080 -D
airflow scheduler -D