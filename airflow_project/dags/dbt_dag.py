import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        # Sửa database thành ll_db
        # Schema ở đây bạn để một schema mặc định bất kỳ (ví dụ ll_bronze hoặc public) 
        # để làm mốc kết nối ban đầu cho dbt. Các model khác sẽ tự động ghi đè schema của chúng.
        profile_args={"database": "ll_db", "schema": "ll_default_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/dbt_project",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    dag_id="dbt_dag",
)