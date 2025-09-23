from cosmos import ProfileConfig, ExecutionConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path


dir_profiles_path = "/usr/local/airflow/.dbt"
dbt_executable_path=str(Path('/usr/local/airflow/dbt_venv/bin/dbt')) # Path dbt executable
profiles_yml_filepath=Path(f"{dir_profiles_path}/profiles.yml").as_posix() # Path dbt profile
project_path=Path('/usr/local/airflow/analytics').as_posix() # Path dbt project

venv_execution_config = ExecutionConfig(
        dbt_executable_path=dbt_executable_path,
        )

profile_config = ProfileConfig(
    profile_name='analytics', # Change the value to the dbt project name
    target_name='dev',
    profiles_yml_filepath=profiles_yml_filepath,
)

project_config = ProjectConfig(project_path)