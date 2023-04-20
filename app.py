import boto3
from dotenv import load_dotenv
import time
import os
import json

load_dotenv('key.env')

REGION = 'us-east-1'
INPUT_BUCKET = "proj3-input"
OUTPUT_BUCKET = "proj3-output"
LAMBDA_FUNCTION = "face-recog-handler"

inputs = {}
outputs = {}


s3_client = boto3.client("s3", region_name='us-east-1', aws_access_key_id=os.environ.get('AWS_KEY'),
                         aws_secret_access_key=os.environ.get('AWS_SECRET'))
lambda_client = boto3.client("lambda", region_name='us-east-1', aws_access_key_id=os.environ.get('AWS_KEY'),
                             aws_secret_access_key=os.environ.get('AWS_SECRET'))

# event = {"Records": [ { "s3": { "object": { "key": "test_1.mp4" } } } ]}


def checkInputBucket():
    objects = s3_client.list_objects_v2(Bucket=INPUT_BUCKET)

    if objects['KeyCount'] > 0:
        for obj in objects['Contents']:
            key = obj['Key']
            if key not in inputs:
                inputs[key] = True
                event = {"Records": [{"s3": {"object": {"key": key}}}]}

                response = lambda_client.invoke(
                    FunctionName=LAMBDA_FUNCTION,
                    InvocationType='Event',
                    Payload=json.dumps(event),
                )

                print("Triggered Lambda for "+key)


def checkOutputBucket():
    objects = s3_client.list_objects_v2(Bucket=OUTPUT_BUCKET)

    if objects['KeyCount'] > 0:
        for obj in objects['Contents']:
            key = obj['Key']
            if key not in outputs:
                outputs[key] = True
                s3_client.download_file(OUTPUT_BUCKET, key, 'outputs/'+key)
                print("Downloaded "+key)


def main():
    while True:
        checkInputBucket()
        checkOutputBucket()
        time.sleep(5)


if __name__ == "__main__":
    main()
