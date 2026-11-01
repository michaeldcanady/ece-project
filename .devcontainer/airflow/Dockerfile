# Airflow on Python 3.9 (the project's Python), with extra libraries resolved against Airflow's own
# constraints so they cannot break the scheduler. Replaces _PIP_ADDITIONAL_REQUIREMENTS, which
# reinstalled unpinned packages on every container start.
FROM apache/airflow:2.7.3-python3.9
ARG AIRFLOW_VERSION=2.7.3
ARG PYTHON_VERSION=3.9
RUN pip install --no-cache-dir "confluent-kafka>=2.3,<3" pyarrow boto3 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN mkdir -p /opt/airflow/data
