export AIRFLOW_HOME=~/PycharmProjects/data-processing-with-airflow/airflow
webserver_pid=$(cat $AIRFLOW_HOME/airflow-webserver.pid)
scheduler_pid=$(cat $AIRFLOW_HOME/airflow-scheduler.pid)

kill $webserver_pid
kill $scheduler_pid
