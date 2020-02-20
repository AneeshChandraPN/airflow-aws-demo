
# Apache Airflow with AWS Services

Author: Aneesh Chandra PN
Date: Dec, 2018

This exercise is designed to complete the below set of activities -
1) Deploy Apache Airflow on an EC2 instance
2) Write an Airflow DAG to build an end to end data pipeline to read raw data from S3, transform the data, prepare aggregates and store the data in Redshift for analysis.
 
## Deploying Airflow on EC2
The following step by step guide provides a quick start to deploying Apache Airflow on AWS EC2 instances. 

### Prerequisites
-   Have access to an AWS account in which your user has  **AdminstratorAccess**
-   This lab should be executed in  **ap-southeast-1**  region. Best is to  **follow links from this guide**
-   Access to a modern browser 😜

### Creating an EC2 Instance
In this step we will navigate to S3 Console and create the EC2 instance used to deploy Apache Airflow on.

Login to AWS Console :  [https://console.aws.amazon.com/console/home?region=ap-southeast-1](https://console.aws.amazon.com/console/home?region=ap-southeast-1)
-  Select **Ubuntu Server 18.04 LTS (HVM)** 
- Select **t2.medium** instance type and click on **Review and Launch**
- Click on **Launch**
- In the next step - **Choose an existing key pair** if you have generated a key pair previously, else create a new key pair that will allow you access to SSH into the EC2 instance
- Click on **Launch Instances**

It will be a minute or two for the instance to be available. You can monitor the status here -
https://ap-southeast-1.console.aws.amazon.com/ec2/v2/home?region=ap-southeast-1#Instances:sort=instanceId


### Install Dependencies
SSH into the EC2 instance created using the key pair (.pem) file and the Public IP address displayed in the console.
```
ssh -i yourawskey.pem ubuntu@public-ip-addr
```

Proceed with installing the following list of modules before we proceed with the Airflow installation.
> Note: 
> We will use PostGres database as the backend DB for Airflow as part of the demo. Apache Airflow supports other database backends such as MySQL, however PostGres is most commonly and widely used.
```
sudo apt-get -y install python-setuptools
sudo apt-get -y update
sudo apt-get -y install python-pip
sudo pip install --upgrade pip
sudo apt-get -y install postgresql postgresql-contrib
```

### Setup PostGres database
Run the below command to connect to postgres database:
```
sudo -u postgres psql
```
Once you see the SQL prompt, run the below set of commands to create a role and grant privileges
```
CREATE ROLE ubuntu;
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES on database airflow to ubuntu;
ALTER ROLE ubuntu SUPERUSER;
ALTER ROLE ubuntu CREATEDB;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ubuntu;
ALTER ROLE ubuntu WITH LOGIN;
```
A final step on the postgres setup is to update the **pg_hba.conf** file. To find the location of the file you can run the below command
```
SHOW hba_file;
```
The location of the pg_hba.conf file should be along the lines of **/etc/postgresql/9.*/main/**). Open the file with a text editor (vi, emacs or nano), and change the ipv4 address to **0.0.0.0/0** and the ipv4 connection method from md5 (password) to **trust**. 
```
set ipv4 to 0.0.0.0/0 trust
```
We will also configure the **postgresql.conf** in the same folder path as above to open the listen address to all ip addresses:
```
listen_addresses = '*'
```
Once the configuration changes are in place for postgres, we can start the servers by running the below commands 
```
sudo service postgresql start
sudo service postgresql reload
```

### Last few things before Airflow Installation
As first step, decide on the folder where the Airflow configurations are to be deployed and set the **AIRFLOW_HOME** environment variable, for ex -
```
mkdir ~/data
export AIRFLOW_HOME=~/data/airflow
```
We will then continue to install some of the dependencies used by different Airflow Modules  `(Not all these need to be used as part of the demo, but might come in handy if you want to explore additional features)`
```
sudo apt-get -y install libmysqlclient-dev
sudo apt-get -y install libssl-dev 
sudo apt-get -y install libkrb5-dev
sudo apt-get -y install libsasl2-dev
```

Next, we will install `psycopg2` so we Airflow can connect to the backend postgres db.
```
sudo pip install psycopg2
```

We will be using the **Celery Executor** along with **RabbitMQ**, instead of the default **Sequential Executor**. So run the next set of commands to setup celery and rabbitmq -
```
sudo pip install 'celery>=3.1.17,<4.0'
sudo apt-get -y install rabbitmq-server
```
Edit the rabbitmq config file **/etc/rabbitmq/rabbitmq-env.conf** to set the node ip to 0.0.0.0
```
NODE_IP_ADDRESS=0.0.0.0
```
and then start the server -
```
sudo service rabbitmq-server start
```

Now we are all set!

### Finally, we are ready to install Airflow

Easiest way to install Apache Airflow is by running the command - 
```
sudo SLUGIFY_USES_TEXT_UNIDECODE=yes pip install apache-airflow
```
Alternately, you can download the source from Github and install the project. 

Airflow by default used SQLite db. In order to change that to the postgres instance setup during the previous steps we will need to modify the **airflow.cfg**. To generate the cfg file, we will run the below command -
```
airflow initdb
```
> Note:
> The config file will be created in the $AIRFLOW_HOME folder, ~/data/airflow/

Lets proceed to change the configurations to customize the Airflow deployment (mainly to use Celery executor and Postgres) - 
```
[core]
executor = CeleryExecutor
sql_alchemy_conn = postgresql+psycopg2://ubuntu@localhost:5432/airflow

[celery]
broker_url = amqp://guest:guest@localhost:5672//
result_backend = amqp://guest:guest@localhost:5672// 
```
If you don’t want the example dags to show up in the webUI, you can set the **load_examples** variable to False. 

Once the changes are in place, we will run the initdb command once more to create all the necessary tables in postgres
```
airflow initdb
```

We have now completed all the configuration changes and setup all required dependencies. Its time to start the airflow services (-D for daemon mode)
```
airflow webserver -D
airflow scheduler -D
airflow worker -D
```

The Airflow webserver is now running and accesible on the default port **8080**.

> Note:
> In order to access the Airflow Web UI, we will need to modify the security group attached with the EC2 instance to allow HTTP traffic on this port. Navigate to the EC2 instance on the Web Console, click on the security group attached to the instance, and edit the **Inbound** rules to a **Custom TCP rule** to allow traffic to port **8080** and save the changes.

## Data Pipeline Architecture
![Airflow Pipeline Architecture](    
https://nanhyama-public.s3-ap-southeast-1.amazonaws.com/nanhyama-lab-files/apache-airflow-demo/AWS_Airflow.jpg)

Now that we have setup an Airflow instance on EC2, lets look at how we can build a data pipeline incorporating the following steps  -

- S3 Sensor that waits for raw files to arrive into a predefined S3 bucket prefix
- A Glue job which transforms the raw data into a processed data format while performing data file conversions
- An EMR job to aggregate the data
- S3-to-Redshift copy of the aggregated data

Apache Airflow has several components when it comes to setting up a DAG to schedule a specific or a collection of tasks, such as Operators, Sensors, Hooks, Tasks, Connections and others. You can read more here -
https://airflow.apache.org/concepts.html

You can get familiar with the components while we work towards setting up a DAG for the data pipeline.

## Preparing the data set and Scripts

For this demo, we will work with the NYC taxi ride open data set. Details about the data set and the download instructions are available in the link -
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
We will not copy the data over to S3 as yet. We will hold on to this step until we have setup the DAG in Airflow, and later perform a S3 copy to trigger the S3 Sensor in the DAG.

There are a few scripts we will use during the course of the exercise. The scripts can be downloaded from [here](https://nanhyama-public.s3-ap-southeast-1.amazonaws.com/nanhyama-lab-files/apache-airflow-demo/airflow-emr-example.zip)

Also, as a preparation step, from the Airflow UI we need to modify the **aws_default** connection to reflect the right region where the demo is being deployed. (ap-southeast-1)

> Note [Optional]:
> Modify Redshift connection if you want to implement the last step of copying the processed data into your Redshift cluster

## Starting with a DAG definition

As first steps in defining the DAG, lets import all the required modules (Operators and Sensors) that we will use as part of the data pipeline -
```
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
```
Next we will define the default set of parameters for the Airflow DAG. We will also specify the S3 bucket name where the data and the scripts will be stored.
```
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
```
We will then proceed to create a DAG object using the default arguments, and specifying the schedule interval at which the DAG should be executed.
```
dag = DAG(  
    'data_pipeline',  
  default_args=default_args,  
  dagrun_timeout=timedelta(hours=2),  
  schedule_interval='0 3 * * *'  
)
```

## S3 Sensor
We will use the **s3_prefix_sensor** available in Apache Airflow to write a task in the DAG that waits for objects to be available in S3 before executing next set of tasks.
```
s3_sensor = S3PrefixSensor(  
    task_id='s3_sensor',  
  bucket_name=S3_BUCKET_NAME,  
  prefix='raw/green',  
  dag=dag  
)
```

## Glue Job Operator
Before proceeding with this step, we will create a Glue catalog table for the raw data and also create the Glue Job script which will be included as part of the data pipeline. 

> Glue Catalog Table script (can be executed from Athena UI):
```
CREATE DATABASE nyc;  
  
CREATE EXTERNAL TABLE nyc.raw_green(  
  `vendorid` bigint,  
  `lpep_pickup_datetime` string,  
  `lpep_dropoff_datetime` string,  
  `store_and_fwd_flag` string,  
  `ratecodeid` bigint,  
  `pulocationid` bigint,  
  `dolocationid` bigint,  
  `passenger_count` bigint,  
  `trip_distance` double,  
  `fare_amount` double,  
  `extra` double,  
  `mta_tax` double,  
  `tip_amount` double,  
  `tolls_amount` double,  
  `ehail_fee` string,  
  `improvement_surcharge` double,  
  `total_amount` double,  
  `payment_type` bigint,  
  `trip_type` bigint)  
ROW FORMAT DELIMITED  
  FIELDS TERMINATED BY ','  
STORED AS INPUTFORMAT  
  'org.apache.hadoop.mapred.TextInputFormat'  
OUTPUTFORMAT  
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  
LOCATION  
  's3://nanhyama-nyc/raw/green/'  
TBLPROPERTIES (  
  'classification'='csv',  
  'columnsOrdered'='true',  
  'compressionType'='none',  
  'delimiter'=',',  
  'skip.header.line.count'='1',  
  'typeOfData'='file');
  ```

Next, we will import the job script into Glue via the AWS Console.
To complete this activity, follow the below set of steps - 
- Edit the Glue job script **glue_job.py** (previously downloaded) to set the bucket name in the data sink step.
- Copy the modified job script to S3 path **s3://<your-bucket-name/scripts/**
- Login to the AWS Console - https://ap-southeast-1.console.aws.amazon.com/glue/home?region=ap-southeast-1#etl:tab=jobs
- Add Job by providing the following details 
	- Name - **nyc_raw_to_transform**
	- IAM Role - **AWSGlueServiceRoleDefault**
	- Select - **An existing script that you provide**
	- ETL Language - **Python**
	-  S3 path where the script is stored - **s3://<your-bucket-name/scripts/glue_job.py**
	- **Next**, **Next**, **Save Job and Edit Script**

Currently there is no Glue Operator shipped with the latest stable release of Apache Airflow. However, it is fairly straight forward to write a custom Operator using the existing AWS hook and boto3 library. The below scripts are included as part of the code base downloaded previously.

**aws_glue_job_hook.py** - consists of the required modules to interact with the Glue API using the AWS hook. The two main hook methods defined in this script are used to initialize a Glue job, and wait for job completion once a job has been submitted through the API.
**aws_glue_job_operator.py** - consists of a method to execute the Glue Job using the hook.

Using the new Glue operator and hook, the DAG task can be written as shown below to invoke an existing Glue job and wait for its completion.
```  
aws_glue_task = AWSGlueJobOperator(  
    task_id="glue_task",  
  job_name='nyc_raw_to_transform',  
  iam_role_name='AWSGlueServiceRole',  
  dag=dag) 
```

## EMR job

Apache Airflow has a default AWS Hook which can be used to interact with different services in Amazon Web Services. It also comes shipped with EMR operators and sensors (contributions from the developer community). 

The configurations required for the EMR cluster to be created will by default be picked from the **emr_default** connection. You can either edit the **emr_default** from the Airflow UI or define a override as shown below -
```
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
```
Next, we will define the Job steps that will be submitted to the EMR cluster.
 ```
S3_URI = "s3://{}/dags/scripts".format(S3_BUCKET_NAME)  
  
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
  'Args': ['spark-submit', '/home/hadoop/nyc_aggregations.py']  
        }  
    }  
] 
 ``` 
Once we have created the definitions for the EMR cluster and the Spark job, its time to define the tasks that will be part of the DAG.
The first task is to create the EMR cluster
```
cluster_creator = EmrCreateJobFlowOperator(  
    task_id='create_emr_cluster',  
  job_flow_overrides=JOB_FLOW_OVERRIDES,  
  aws_conn_id='aws_default',  
  emr_conn_id='emr_default',  
  dag=dag  
) 
```  
Next we will create a task that will submit the step (Spark job) to the EMR cluster once the cluster has been created -
```
step_adder = EmrAddStepsOperator(  
    task_id='add_step',  
  job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",  
  aws_conn_id='aws_default',  
  steps=SPARK_TEST_STEPS,  
  dag=dag  
) 
```
We will need to add a Watch task to wait for the Job to reach a completion stage (Successful or failed execution) before we terminate the cluster.
```
step_checker = EmrStepSensor(  
    task_id='watch_step',  
  job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",  
  step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",  
  aws_conn_id='aws_default',  
  dag=dag  
)
```
After the Job has completed its execution the DAG will run the cluster termination task 
```
cluster_remover = EmrTerminateJobFlowOperator(  
    task_id='remove_cluster',  
  job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",  
  aws_conn_id='aws_default',  
  dag=dag  
)
```

## S3 to Redshift Copy (Optional)
It is quite easy to add a final step in the DAG to copy the aggregated data from S3 to a table defined in Redshift.
As a pre-requisite for this, the **redshift_default** connection needs to be set to point to the redshift cluster.

```
copy_agg_to_redshift = S3ToRedshiftTransfer(  
    task_id='copy_to_redshift',  
  schema='nyc',  
  table='agg_green_rides',  
  s3_bucket=S3_BUCKET_NAME,  
  s3_key='aggregate/agg-green-rides')
 ```

## Stitching it all together

The last bit of code in the DAG is to define the flow/dependencies between all the above tasks. We can setup the dependencies as shown below -
```
s3_sensor >> aws_glue_task >> cluster_creator >> step_adder >> step_checker >> cluster_remover >> copy_agg_to_redshift
```
That brings us to the end of the DAG definition. We can put together all the steps into a individual DAG file **data_pipeline.py** and copy the script into the **$AIRFLOW_HOME/dags** folder.
Once the script is deployed, it can be viewed from the Airflow UI.

## Data Pipeline in Action

The DAG will be initially set to disabled state by default. We can either enable the DAG to be picked up by the scheduler or we can also run the  DAG manually (if we are using a Celery Executor).

![Airflow Pipeline Architecture](  
https://nanhyama-public.s3-ap-southeast-1.amazonaws.com/nanhyama-lab-files/apache-airflow-demo/airflow-dag.png)

To trigger the DAG, click on the first Task i.e **s3_sensor** and click on **Run**.

![Airflow Pipeline Architecture](  
https://nanhyama-public.s3-ap-southeast-1.amazonaws.com/nanhyama-lab-files/apache-airflow-demo/airflow-dag-run.png)

The Airflow DAG will not wait for the files to arrive in S3 in the path **s3://your-bucket-name/raw/green/**. To initiate the DAG run, copy over the previously downloaded files into the specified path using the AWS S3 Cli command.

Once the files arrive in the folder, the **s3_sensor** task will complete, and the execution will be passed over to the next set of tasks in the DAG.

In case of any issues the task will be marked as Failed (Red), you can click on the task and **View Log** to identify the reason for failure.
