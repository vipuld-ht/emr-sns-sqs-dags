from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator, EmrAddStepsOperator, EmrTerminateJobFlowOperator
from airflow.operators.python import PythonOperator
import boto3
import pendulum
# AWS Config
AWS_CONN_ID = "aws_default"

# EMR Cluster Configuration
JOB_FLOW_OVERRIDES = {
    "Name": "Airflow-EMR-Cluster",
    "ReleaseLabel": "emr-6.5.0",
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    },
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

# EMR Job Steps
EMR_STEPS = [
    {
        "Name": "Run Spark Job",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", "--deploy-mode", "cluster", "s3://insightrag-job-config/emr/spark_manage_file.py"],
        },
    }   
]

# SNS and SQS Config
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:867344468918:emr-sns-topic"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/867344468918/emr-sns-sqs"

def send_sns_notification(**kwargs):
    client = boto3.client("sns")
    response = client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message="EMR Job Completed Successfully",
        Subject="Airflow Notification"
    )
    return response

def send_sqs_message(**kwargs):
    client = boto3.client("sqs")
    response = client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody="EMR Job Finished!"
    )
    return response

# DAG Definition
with DAG(
    dag_id="emr_sns_sqs_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz='UTC'),
    schedule="@daily",
    catchup=False,
) as dag:

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        aws_conn_id=AWS_CONN_ID,
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        aws_conn_id=AWS_CONN_ID,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=EMR_STEPS
    )

    send_sns = PythonOperator(
        task_id="send_sns_notification",
        python_callable=send_sns_notification
    )

    send_sqs = PythonOperator(
        task_id="send_sqs_message",
        python_callable=send_sqs_message
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        aws_conn_id=AWS_CONN_ID,
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}"
    )

    # DAG Dependencies
    create_emr_cluster >> add_steps >> terminate_cluster
    terminate_cluster >> send_sns >> send_sqs