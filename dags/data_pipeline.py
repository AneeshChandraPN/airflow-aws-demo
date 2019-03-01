from datetime import timedelta
import airflow
from airflow import DAG
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from awsairflowlib.contrib.operators.aws_glue_job_operator import AWSGlueJobOperator

S3_BUCKET_NAME = "nanhyama-nyc"

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
    task_id="glue_task",
    job_name='nyc_raw_to_transform',
    iam_role_name='AWSGlueServiceRoleDefault',
    dag=dag)

S3_URI = "s3://{}/scripts/nyc_aggregations.py".format(S3_BUCKET_NAME)

SPARK_TEST_STEPS = [
    {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', S3_URI, '/home/hadoop/']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/nyc_aggregations.py', 's3n://nanhyama-nyc/transformed/green/',  's3n://nanhyama-nyc/aggregate/agg-green-rides/']
        }
    }
]

execution_date = "{{ execution_date }}"

JOB_FLOW_OVERRIDES = {
    'Name': 'Data-Pipeline-' + execution_date,
    "Instances": {
        "Ec2KeyName": "myaws-key",
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "r3.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "r3.xlarge",
                "InstanceCount": 2
            }
        ],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": True
    }
}

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

copy_agg_to_redshift = S3ToRedshiftTransfer(
    task_id='copy_to_redshift',
    schema='nyc',
    table='agg_green_rides',
    s3_bucket=S3_BUCKET_NAME,
    s3_key='aggregate/agg-green-rides',
    dag=dag)

# construct the DAG by setting the dependencies
s3_sensor >> aws_glue_task >> cluster_creator >> step_adder >> step_checker >> cluster_remover >> copy_agg_to_redshift
