FROM apache/airflow:2.5.0

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip \
    & pip install --no-cache-dir --user -r /requirements.txt

ENV PYTHONPATH=/opt/airflow
