# Airflow on Python 3.9 (the project's Python), with extra libraries resolved against Airflow's own
# constraints so they cannot break the scheduler. Replaces _PIP_ADDITIONAL_REQUIREMENTS, which
# reinstalled unpinned packages on every container start.
FROM apache/airflow:2.7.3-python3.9
ARG AIRFLOW_VERSION=2.7.3
# Not named PYTHON_VERSION: the base image exports PYTHON_VERSION=3.9.x as an env var, which
# overrides an ARG of the same name, and constraints files exist only per minor version.
ARG CONSTRAINTS_PYTHON=3.9
RUN pip install --no-cache-dir "confluent-kafka>=2.3,<3" pyarrow boto3 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${CONSTRAINTS_PYTHON}.txt"
RUN mkdir -p /opt/airflow/data
