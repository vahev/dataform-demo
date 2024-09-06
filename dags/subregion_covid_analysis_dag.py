from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dataform_covid_analysis',
    default_args=default_args,
    description='Orchestrates Dataform workflow for COVID-19 analysis',
    schedule_interval=timedelta(days=1),  # Adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    def dataform_success_callback(context):
        """
        Callback function to handle successful Dataform workflow invocation.
        """
        print("Dataform workflow completed successfully!")

    dataform_task = DataformCreateWorkflowInvocationOperator(
        task_id='trigger_dataform_workflow',
        project_id='your-project-id',  # Replace with your GCP project ID
        region='your-region',  # Replace with your Dataform region
        repository_id='your-repository-id',  # Replace with your Dataform repository ID
        compilation_result_id='your-compilation-result-id',  # Replace with the latest successful compilation result ID
        on_success_callback=dataform_success_callback,  # Optional callback function
    )