from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='emr_on_eks_example',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    submit_spark_job = EmrContainerOperator(
        task_id='submit_spark_job',
        virtual_cluster_id='3661txnkugj15sb9h3lz56ir1',  # Replace with your EMR virtual cluster ID
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

    submit_spark_job