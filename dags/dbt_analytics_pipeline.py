# dags/my_pipeline.py

import sys

sys.path.insert(0, "/usr/local/airflow/")  # Import the modules from the path

from include.profiles import profile_config, venv_execution_config, project_config
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup
from airflow import DAG
from dags.utils.s3_clickhouse import *

dag = DAG(dag_id="food_truck_data_pipeline", schedule=None, catchup=False)


# Set the configuration to DbtTaskGroup
dbt_task_group = DbtTaskGroup(
    group_id="dbt_task_dag",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=venv_execution_config,
    dag=dag,
)


create_tables_task = PythonOperator(
    task_id="create_raw_tables",
    python_callable=create_raw_tables,
    dag=dag,
)

ingestion_tasks = []
for i, table_config in enumerate(TABLE_CONFIGS):
    task = PythonOperator(
        task_id=f'ingest_{table_config["table_name"]}',
        python_callable=ingest_table_from_s3,
        op_kwargs={"table_config": table_config},
        dag=dag,
    )
    ingestion_tasks.append(task)


end_empty_task = EmptyOperator(task_id="end", dag=dag)

# Pipeline
create_tables_task >> ingestion_tasks >> dbt_task_group >> end_empty_task
