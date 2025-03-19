from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.emr import EmrHook
import boto3
import pendulum

# AWS Config
AWS_CONN_ID = "aws_default"
REGION_NAME = "us-east-1"

# EMR Cluster Configuration
JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-EMR-Cluster-with-hooks",
    "ReleaseLabel": "emr-7.1.0",
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master node", "Market": "ON_DEMAND", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
            {"Name": "Core nodes", "Market": "ON_DEMAND", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
    "LogUri": "s3://aws-logs-867344468918-us-east-1/elasticmapreduce/"
}

# EMR Job Steps
EMR_STEPS = [
    {
        "Name": "Run Spark Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster", "s3://insightrag-job-config/emr/csv_salary_filter.py"]
        },
    }
]

# SNS and SQS Config
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:867344468918:emr-sns-topic"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/867344468918/emr-sns-sqs"

def create_emr_cluster(**kwargs):
    hook = EmrHook(aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME)
    response = hook.create_job_flow(job_flow_overrides=JOB_FLOW_OVERRIDES)
    kwargs['ti'].xcom_push(key='job_flow_id', value=response["JobFlowId"])

def add_steps_to_emr(**kwargs):
    hook = EmrHook(aws_conn_id=AWS_CONN_ID, region_name=REGION_NAME)
    job_flow_id = kwargs['ti'].xcom_pull(task_ids='create_emr_cluster', key='job_flow_id')
    response = hook.add_job_flow_steps(job_flow_id=job_flow_id, steps=EMR_STEPS, wait_for_completion=True)
    hook.test_connection()


def send_sns_notification(**kwargs):
    client = boto3.client("sns")
    client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message="EMR Job Completed Successfully",
        Subject="Airflow Notification"
    )

def send_sqs_message(**kwargs):
    client = boto3.client("sqs")
    client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody="EMR Job Finished!"
    )

# DAG Definition
with DAG(
    dag_id="emr_sns_sqs_dag_with_hooks",
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    schedule="@daily",
    catchup=False,
) as dag:

    create_emr_cluster_task = PythonOperator(
        task_id="create_emr_cluster",
        python_callable=create_emr_cluster,
	trigger_rule='all_success'
    )

    add_steps_task = PythonOperator(
        task_id="add_steps",
        python_callable=add_steps_to_emr,
	trigger_rule='all_success'
    )

    send_sns_task = PythonOperator(
        task_id="send_sns_notification",
        python_callable=send_sns_notification,
	trigger_rule='all_success'
    )

    send_sqs_task = PythonOperator(
        task_id="send_sqs_message",
        python_callable=send_sqs_message
    )

    #create_emr_cluster_task >> add_steps_task
    create_emr_cluster_task >> add_steps_task >> send_sns_task >> send_sqs_task
