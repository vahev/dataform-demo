from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator
)
from airflow.models import Variable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DAG_ID = "dataform_demo_workflow"
PROJECT_ID = "TODO: project_id"
REPOSITORY_ID = "quickstart-repository"
REGION = "TODO: region"
GIT_COMMITISH = "main"

with DAG(
    "dataform_covid_analysis",
    default_args=default_args,
    description="Orchestrates Dataform workflow for COVID-19 analysis",
    schedule_interval=timedelta(days=1),  # Adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    def get_dataform_config(environment):
        """
        Retrieves Dataform configuration based on the environment.
        """
        return {
            'project_id': Variable.get(f'dataform_project_id'),
            'region': Variable.get(f'dataform_region'),
            'repository_id': Variable.get(f'dataform_repository_id'),
            'git_commitish': Variable.get(f'dataform_environment'),
        }

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id="create_workflow_invocation",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

    create_compilation_result >> create_workflow_invocation
