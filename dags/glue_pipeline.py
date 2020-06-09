from datetime import timedelta
import airflow
from airflow import DAG
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from awsairflowlib.contrib.operators.aws_glue_job_operator import AWSGlueJobOperator

S3_BUCKET_NAME = "<s3 bucket name>"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Initialize the DAG
dag = DAG(
    'data_pipeline',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval='0 3 * * *'
)


s3_sensor = S3PrefixSensor(
    task_id='s3_sensor',
    bucket_name=S3_BUCKET_NAME,
    prefix='raw/green',
    dag=dag
)

aws_glue_task = AWSGlueJobOperator(
    task_id="aws_glue_task",
    job_name='<glue job name>',
    iam_role_name='AWSGlueServiceRoleDefault',
    dag=dag)

# construct the DAG by setting the dependencies
s3_sensor >> aws_glue_task
