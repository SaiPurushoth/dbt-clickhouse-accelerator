FROM astrocrpublic.azurecr.io/runtime:3.0-9


RUN python -m venv /usr/local/airflow/dbt_venv && \
    /usr/local/airflow/dbt_venv/bin/pip install dbt-core==1.8.2 dbt-clickhouse==1.8.2
