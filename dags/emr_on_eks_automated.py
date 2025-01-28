from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='emr_on_eks_automated',
    default_args=default_args,
    schedule_interval=None,
) as dag: 

    # Function to create EMR virtual cluster

    def create_emr_virtual_cluster(**kwargs):
        # Use the Airflow AWS connection
        aws_hook = AwsBaseHook(aws_conn_id='aws_default', client_type='emr-containers')
        client = aws_hook.get_client()
        try:
            response = client.create_virtual_cluster(
                name='dco-emr-dev',
                containerProvider={
                    'id': 'dco-cluster-dev',
                    'type': 'EKS',
                    'info': {
                        'eksInfo': {
                            'namespace': 'emr'
                        }
                    }
                }
            )
            virtual_cluster_id = response['id']
            kwargs['ti'].xcom_push(key='virtual_cluster_id', value=virtual_cluster_id)
            print(f"Successfully created EMR virtual cluster: {virtual_cluster_id}")
            return virtual_cluster_id
        except ClientError as e:
            error_message = f"Failed to create EMR virtual cluster: {e.response['Error']['Message']}"
            print(error_message)
            raise AirflowException(error_message)

    # Function to terminate EMR virtual cluster
    def terminate_emr_virtual_cluster(**kwargs):
        ti = kwargs['ti']
        virtual_cluster_id = ti.xcom_pull(task_ids='create_emr_virtual_cluster', key='virtual_cluster_id')
        client = boto3.client('emr-containers')
        try:
            client.delete_virtual_cluster(id=virtual_cluster_id)
            print(f"Terminated EMR virtual cluster: {virtual_cluster_id}")
        except ClientError as e:
            raise AirflowException(f"Failed to terminate EMR virtual cluster: {e}")

    # Task to create EMR virtual cluster
    create_cluster = PythonOperator(
        task_id='create_emr_virtual_cluster',
        python_callable=create_emr_virtual_cluster,
        provide_context=True,
    )

    # Task to submit Spark job
    submit_spark_job = EmrContainerOperator(
        task_id='submit_spark_job',
        virtual_cluster_id="{{ ti.xcom_pull(task_ids='create_emr_virtual_cluster', key='virtual_cluster_id') }}",
        execution_role_arn='arn:aws:iam::343218188894:role/emr-job-role',  # Replace with your IAM role ARN
        release_label='emr-6.5.0-latest',  # EMR release version
        job_driver={
            'sparkSubmitJobDriver': {
                'entryPoint': 's3://emr-bucket-cluster/hello_world.py',  # Replace with your Spark job S3 path
                'sparkSubmitParameters': '--conf spark.executor.instances=2 --conf spark.executor.memory=2G --conf spark.driver.memory=2G --conf spark.executor.cores=2'
            }
        },
        configuration_overrides={
            'applicationConfiguration': [
                {
                    'classification': 'spark-defaults',
                    'properties': {
                        'spark.dynamicAllocation.enabled': 'false'
                    }
                }
            ],
        },
        name='airflow-spark-job',
    )

    # Task to terminate EMR virtual cluster
    terminate_cluster = PythonOperator(
        task_id='terminate_emr_virtual_cluster',
        python_callable=terminate_emr_virtual_cluster,
        provide_context=True,
    )

    # Define task dependencies
    create_cluster >> submit_spark_job >> terminate_cluster
