from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.utils.dates import days_ago
import pendulum

# Define job flow overrides (Ensure IAM roles are correct)
job_flow_overrides = {
    "Name": "MyEMRCluster",
    "ReleaseLabel": "emr-6.7.0",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",  # Ensure this role exists
    "ServiceRole": "EMR_DefaultRole"       # Ensure this role exists
}

# Define the DAG
dag = DAG(
    dag_id='emr_job_flow',
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    schedule='@daily',    # Correct parameter
    catchup=False,
)

# Define the tasks/operators
create_cluster = EmrCreateJobFlowOperator(
    task_id='create_cluster',
    job_flow_overrides=job_flow_overrides,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag,
)

# Define Spark step
spark_step = {
    'Name': 'Run Spark',
    'ActionOnFailure': 'TERMINATE_CLUSTER',
    'HadoopJarStep': {
        'Jar': 'command-runner.jar',
        'Args': ['spark-submit', 's3://my-bucket/spark-job.py'],
    },
}

# Add steps to EMR cluster
add_step = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    steps=[spark_step],
    aws_conn_id='aws_default',
    dag=dag,
)

# Terminate EMR cluster
terminate_cluster = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)

# Define DAG dependencies
create_cluster >> add_step >> terminate_cluster
