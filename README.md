# Airflow EMR DAGs

## Overview
This repository contains Apache Airflow DAGs for managing AWS EMR clusters. The DAGs automate the creation of EMR clusters, execution of steps, monitoring, and termination.

## Prerequisites
- Apache Airflow installed locally or on an EC2 instance
- AWS credentials configured for EMR access
- Python scripts for Spark jobs (stored in the `spark_scripts` folder)
- Airflow connections configured for AWS (IAM roles, SNS, etc.)

## DAGs and Their Functionality

1. **`emr_dag.py`**
   - Creates an EMR cluster
   - Adds a step to the cluster
   - Terminates the EMR cluster upon completion

2. **`emr_sns_dag.py`**
   - Similar to `emr_dag.py`
   - Includes SNS notifications upon completion

3. **`emr_sns_dag_with_sensor.py`**
   - Creates an EMR cluster
   - Adds a step to the cluster
   - Waits for the step to complete using a sensor
   - Terminates the EMR cluster upon completion

4. **`emr_sns_hook_dag.py`**
   - Creates an EMR cluster
   - Adds a step to the cluster
   - Waits for the step to complete using a sensor
   - Handles failure scenarios post-step execution
   - Terminates the EMR cluster upon completion

## Spark Scripts
The `spark_scripts` folder contains Python scripts that are executed as steps in the EMR cluster. These scripts define the Spark jobs that process data.

## Running the DAGs
1. Start Airflow on your local machine or EC2 instance.
2. The DAGs will be automatically loaded.
3. In the Airflow UI, select a DAG and trigger execution.
4. Monitor execution logs:
   - Click on the DAG execution in the Airflow UI.
   - View logs by selecting a task node in the Graph view.

## Monitoring and Debugging
- Use Airflow logs to track execution status.
- Check AWS EMR logs for detailed Spark job execution insights.
- Ensure AWS permissions are correctly set for the EMR and Airflow integration.

## Conclusion
These DAGs automate EMR cluster management, ensuring efficient execution of Spark jobs with monitoring capabilities. Modify and extend them as per your project requirements.

