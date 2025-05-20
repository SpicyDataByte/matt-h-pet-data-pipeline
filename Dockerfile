FROM apache/airflow:2.9.1-python3.9

USER root

RUN mkdir -p /opt/airflow/data/cleaned \
    && mkdir -p /opt/airflow/data/raw \
    && mkdir -p /opt/airflow/data/validated \
    && chown -R airflow: /opt/airflow/data

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./dags /opt/airflow/dags
COPY ./data/data.xlsx /opt/airflow/data/
