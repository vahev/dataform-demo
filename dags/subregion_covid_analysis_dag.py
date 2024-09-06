import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "dataform_covid_analysis",
    default_args=default_args,
    description="Orchestrates Dataform workflow for COVID-19 analysis",
    schedule_interval=timedelta(days=1),  # Adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    create_compilation = DataformCreateCompilationResultOperator(
        task_id="create_compilation",
        project_id=os.getenv('GCP_PROJECT'),
        region=os.getenv('dataform_region'),
        repository_id=os.getenv('dataform_repository_id'),
        compilation_result={
            "git_commitish": os.getenv('dataform_environment'),
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=os.getenv('GCP_PROJECT'),
        region=os.getenv('dataform_region'),
        repository_id=os.getenv('dataform_repository_id'),
        workflow_invocation={
            "compilation_result": (
                "{{ task_instance.xcom_pull('create_compilation')['name'] }}"
            )
        },
    )

    create_compilation >> create_workflow_invocation
