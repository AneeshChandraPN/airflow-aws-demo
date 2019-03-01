from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from botocore.exceptions import ClientError
from datetime import datetime


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def put_item(job):
    time_marker = "1900-01-01 00:00:00.0000"
    doc = {
            'job': job,
            'info': {
                'last_run': str(datetime.now()),
                'time_marker': time_marker,
                'run_time': decimal.Decimal(0),
                'marker_type': "first run"
            }
        }
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1') #, endpoint_url="http://localhost:8000")
    table = dynamodb.Table('JobTracker')
    try:
        response = table.put_item(Item=doc)
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("PutItem succeeded")
        # print(json.dumps(response, indent=4, cls=DecimalEncoder))
        return 0


def get_item(job):
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1') #, endpoint_url="http://localhost:8000")
    table = dynamodb.Table('JobTracker')

    try:
        response = table.get_item(Key={
            'job': job
        })
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:

        if 'Item' not in response:
            put_item(job)
            response = table.get_item(Key={
                'job': job
            })
            print("New Item Inserted")
            n_item = response['Item']
            return n_item
        else:
            item = response['Item']
            return item


def update_item(job, run_time, time_marker):
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1') #, endpoint_url="http://localhost:8000")
    table = dynamodb.Table('JobTracker')

    try:
        response = table.update_item(
            Key={
                'job': job
            },
            UpdateExpression="set info.last_run = :l, info.time_marker=:t, info.run_time=:r, info.marker_type=:u",
            ExpressionAttributeValues={
            ':r': decimal.Decimal(run_time),
            ':u': "Incremental run",
            ':l': time_marker,
            ':t': str(datetime.now())
        },
            ReturnValues="UPDATED_NEW"
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("UpdateItem succeeded:")
        return 0


def delete_item(job):
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-1') #, endpoint_url="http://localhost:8000")
    table = dynamodb.Table('JobTracker')
    try:
        response = table.delete_item(Key={
            'job': job
        })
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("DeleteItem succeeded:")
        print(json.dumps(response, indent=4, cls=DecimalEncoder))


if __name__ == '__main__':
    job = "Job2"

    item = get_item(job)
    print(json.dumps(item, indent=4, cls=DecimalEncoder))

    # run_time = 5
    # time_marker = str(datetime.now())
    # update_item(job, run_time, time_marker)
    #
    # item = get_item(job)
    # print(json.dumps(item, indent=4, cls=DecimalEncoder))

    # delete_item('JobTracker', doc2)
