from pypardot.client import PardotAPI
import pandas as pd
import boto3
import time
import json
import datetime as dt
from time import sleep
from os import environ
from operator import itemgetter
from smart_open import smart_open

# TODO, figure out if that nested while loop works
# TODO, setup exception handler for batch and for task
# TODO, instead of using max ID, need to use max update datetime from the data to filter pardot.
# I guess just query the data and keep track of the max time to store in a file
# TODO, need to have a query of snowflake if a max doesn't exist in the bucket
# TODO, maybe also have it check snowflake and take the larger value of the two incase an error happens with S3 write
# its ok to have duplicates, snowflake handles. But not ideal
# required format for 'update_after' in pardot query -> 2021-04-15 00:00:01

# Gather environmental variables
EMAIL = environ.get("pardotEmail")
USER_KEY = environ.get("pardotUserKey")
SF_CONSUMER_KEY = environ.get("pardotSfConsumerKey")
SF_CONSUMER_SECRET = environ.get("pardotSfConsumerSecret")
SF_REFRESH_TOKEN = environ.get("pardotSfRefreshToken")
BUSINESS_UNIT_ID = environ.get("pardotSfBusinessUnitID")
VERSION = environ.get("pardotVersion")
BUCKET = environ.get("s3FileStore")
SECONDS_TIMEOUT: int = 60 * 13  # 13 Minutes


# Placeholder values


def lambda_handler(event, context):

    # Time lambda started
    start_time = dt.datetime.now()

    # The type of data being pulled
    data_type = event["Metadata"].get("DataType")

    print(f"Starting Pardot pull for {data_type}")

    # Timeout for loop pulling data.
    # We need it to be less than 15 so the batch lambda doesn't time out
    timeout = time.time() + SECONDS_TIMEOUT

    # Execute the function and pull out the total length of data pulled
    data_length, last_modified_date = execute(data_type, timeout)

    # Time lambda finished
    end_time = dt.datetime.now()

    print(
        f"""Finished Pardot pull for {data_type}.
    Process took {end_time-start_time} seconds to pull.
    {data_length} were pulled"""
    )

    # Return results to batch script
    # TODO We should have the response return that max last_modified_date found in the data
    return {
        "statusCode": 200,
        "body": {
            "DataType": data_type,
            "Result": "Successful",
            "NumberofRecords": data_length,
            "RunTime": str(end_time - start_time),
            "LastModifiedDate": str(last_modified_date.isoformat()),
        },
    }


def execute(data_type, timeout) -> (int, dt.datetime):

    # Create class for writing to s3 bucket
    class S3JsonBucket:
        def __init__(self, bucket_name):
            self.bucket = boto3.resource("s3").Bucket(bucket_name)

        def load(self, key):
            return json.load(self.bucket.Object(key=key).get()["Body"])

        def dump(self, key, obj):
            return self.bucket.Object(key=key).put(Body=json.dumps(obj))

    # Instantiate Pardot Client
    p = PardotAPI(
        email=EMAIL,
        user_key=USER_KEY,
        sf_consumer_key=SF_CONSUMER_KEY,
        sf_consumer_secret=SF_CONSUMER_SECRET,
        sf_refresh_token=SF_REFRESH_TOKEN,
        business_unit_id=BUSINESS_UNIT_ID,
        version=VERSION,
    )

    try:
        data_client = getattr(p, data_type)
    except AttributeError:
        raise Exception(f"{data_type} is not a valid selection!")

    # Do we need separate ones for different files?
    jsbucketMaxID = boto3.resource("s3").Bucket(BUCKET)

    # TODO, this needs to be re-written to focus on max modified date instead of MAXID
    # TODO, we also might have the batch job handle this process
    # TODO and pass as a task parameter

    # Iterate through files to get the latest max id
    maxIdFiles = jsbucketMaxID.objects.filter(Prefix=f"{data_type}/maxid")
    last_modified_date = dt.datetime(1939, 9, 1)

    if len(maxIdFiles) > 0:
        last_modified_file = max(maxIdFiles, key=lambda k: k["last_modified"])

        last_modified_date = last_modified_file["last_modified"]
        last_modified_file_key = last_modified_file["file"].key

        # Initialize maxid from retrieved file
        with smart_open(f"s3://{BUCKET}/{last_modified_file_key}") as fh:
            maxid = json.load(fh)

        print("initial maxid=", maxid)
    else:
        # if a max id file doesn't exist yet, then start at 0
        maxid = 0

    jsbucketDump = S3JsonBucket(BUCKET)
    total_records = 0

    # So we want this to stop running after 13 minutes. We want to guarentee that batch runner doesn't timeout
    # waiting for this
    # TODO, figure out if this while loop works

    i = 0

    while (time.time() < timeout) and (
        i <= data_client.query(id_greater_than=maxid)["total_results"] - 1
    ):
        # This query needs to be changed to us last_updated_dt. We also want to have this passed
        # as a parameter by the batch
        FORMAT_WRITETIME = "%Y%m%d-%H%M%S"

        data = data_client.query(format="json", sort_by="id", id_greater_than=maxid)

        file_name = (
            f"test/{data_type}/{data_type}_{time.strftime(FORMAT_WRITETIME)}.json"
        )
        jsbucketDump.dump(file_name, data)
        print(f"Sent {file_name} to {BUCKET} with {len(data.keys())} records")

        writetime = time.strftime(FORMAT_WRITETIME)
        jsbucketDump.dump(f"{data_type}/{data_type}_{writetime}.json", data)

        # write current maxid to S3 - If lambda times out, next execution will pickup with this value of maxid.
        maxid = data[data_type][-1]["id"]
        jsbucketDump.dump(f"{data_type}_maxid/maxid_{writetime}.json", maxid)
        print(f"updated maxid={maxid}")

        sleep(1)  # 1 sec delay to insure different filenames

        i = i + 200

        total_records = total_records + len(data.keys())

        if len(data.keys()) < 200 or time.time() > timeout:
            # Timeout is hit or the number of records indicates the end, break loop
            break

    return total_records, last_modified_date
