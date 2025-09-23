import sys
sys.path.insert(0, '/usr/local/airflow/')
from include.profiles import profile_config, venv_execution_config, project_config #, profile_mapping
from cosmos import DbtDag

dag = DbtDag(
    dag_id='dbt_dag',
    project_config=project_config, 
    profile_config=profile_config, # or profile_mapping=profile_mapping,
    execution_config=venv_execution_config,
    start_date=None,
    catchup=False,
)





