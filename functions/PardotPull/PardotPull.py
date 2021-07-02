from functions.prospectaccounts.prospectaccounts import prospectaccounts_func
from pypardot.client import PardotAPI
import pandas as pd							  
import boto3                                  
import time
import json
from datetime import datetime
from time import sleep
from os import environ
from operator import itemgetter
from smart_open import smart_open

#TODO, figure out if that nested while loop works
#TODO, setup exception handler for batch and for task
#TODO, instead of using max ID, need to use max update datetime from the data to filter pardot. 
    # I guess just query the data and keep track of the max time to store in a file
#TODO, need to have a query of snowflake if a max doesn't exist in the bucket
#TODO, maybe also have it check snowflake and take the larger value of the two incase an error happens with S3 write
    # its ok to have duplicates, snowflake handles. But not ideal
# required format for 'update_after' in pardot query -> 2021-04-15 00:00:01

# Gather environmental variables
email = environ.get('pardotEmail')
user_key = environ.get('pardotUserKey')
sf_consumer_key = environ.get('pardotSfConsumerKey')
sf_consumer_secret = environ.get('pardotSfConsumerSecret')
sf_refresh_token = environ.get('pardotSfRefreshToken')
business_unit_id = environ.get('pardotSfBusinessUnitID')
version = environ.get('pardotVersion')
bucket = environ.get('s3FileStore')

# Placeholder values
maxid=0
i=0
last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None)

def lambda_handler(event, context):
    
    # Time lambda started 
    start_time = datetime.now()

    # The type of data being pulled
    data_type = event['Metadata'].get('DataType')

    print(f'Starting Pardot pull for {data_type}')

    # Timeout for loop pulling data.
    # We need it to be less than 15 so the batch lambda doesn't time out
    timeout = time.time() + 60*13

    # Execute the function and pull out the total length of data pulled
    data_length = execute(data_type,timeout)

    # Time lambda finished
    end_time = datetime.now()
    
    print(f"""Finished Pardot pull for {data_type}. 
    Process took {end_time-start_time} seconds to pull.
    {data_length} were pulled""")

    # Return results to batch script
    # TODO We should have the response return that max last_modified_date found in the data
    return {
        'statusCode': 200,
        'body': {'DataType': data_type, 'Result': 'Successful', 'NumberofRecords': data_length,
        'RunTime':str(end_time-start_time)}
    }

def execute(data_type,timeout):
    # Create class for writing to s3 bucket
    def pull_data(data_type,timeout):
        class S3JsonBucket:
            def __init__(self, bucket_name):
                self.bucket = boto3.resource("s3").Bucket(bucket_name)

            def load(self, key):
                return json.load(self.bucket.Object(key=key).get()["Body"])

            def dump(self, key, obj):
                return self.bucket.Object(key=key).put(Body=json.dumps(obj))

        #Instantiate Pardot Client
        p=PardotAPI(email=email,user_key=user_key,sf_consumer_key=sf_consumer_key,sf_consumer_secret=sf_consumer_secret,sf_refresh_token=sf_refresh_token,business_unit_id=business_unit_id,version=version)

        # Determine what needs to be queried based off type of data
        if data_type == 'visitoractivities':
            data_client = p.visitoractivities

        elif data_type == 'campaigns':
            data_client = p.campaigns

        elif data_type == 'lists':
            data_client = p.lists

        elif data_type == 'prospects':
            data_client = p.prospects

        elif data_type == 'tagobjects':
            data_client = p.tagobjects

        elif data_type == 'tags':
            data_client = p.tags

        elif data_type == 'opportunities':
            data_client = p.opportunities

        elif data_type == 'prospectaccounts':
            data_client = p.prospectaccounts

        elif data_type == 'forms':
            data_client = p.forms

        elif data_type == 'listmemberships':
            data_client = p.listmemberships

        elif data_type == 'visitors':
            data_client = p.visitors

        else :
            
            raise Exception (f'{data_type} is not a valid selection!')

        # Do we need separate ones for different files?
        jsbucketMaxID=boto3.resource("s3").Bucket(bucket)

        # TODO, this needs to be re-written to focus on max modified date instead of MAXID
        # TODO, we also might have the batch job handle this process
        # TODO and pass as a task parameter
        #Iterate through files to get the latest max id
        maxIdFiles = jsbucketMaxID.objects.filter(Prefix=f'{data_type}/maxid')
        if len(maxIdFiles)>0:
            modified_dates = [{'file':file,'last_modified':file['last_modified'].replace(tzinfo=None)} for file in maxIdFiles]
            last_modified_date = max(modified_dates['last_modified'], key=lambda ev: ev['last_modified'])
            last_modified_file_key = [file['file'] for file in modified_dates if file['last_modified']==last_modified_date][0].key
            # Initialize maxid from retrieved file
            path='s3://{}/{}'.format(bucket, last_modified_file_key)
            maxid = json.load(smart_open(path))
            print('initial maxid=',maxid)
        else:
            # if a max id file doesn't exist yet, then start at 0
            maxid = 0
        
        jsbucketDump = S3JsonBucket(bucket)
        total_records = 0

        # So we want this to stop running after 13 minutes. We want to guarentee that batch runner doesn't timeout
        # waiting for this
        #TODO, figure out if this while loop works
        while True:
            
            # This query needs to be changed to us last_updated_dt. We also want to have this passed
            # as a parameter by the batch
            while i <= data_client.query(id_greater_than=maxid)['total_results'] -1: 

                data=data_client.query(format='json',sort_by='id',id_greater_than=maxid)

                writetime=time.strftime("%Y%m%d-%H%M%S") 

                file_name = f"test/{data_type}/{data_type}_{writetime}.json"
                jsbucketDump.dump(file_name, data)

                print(f'Sent {file_name} to {bucket} with {len(data.keys())} records')

                writetime=time.strftime("%Y%m%d-%H%M%S") 
                jsbucketDump.dump(f"{data_type}/{data_type}_{writetime}.json", data)

                # write current maxid to S3 - If lambda times out, next execution will pickup with this value of maxid.
                maxid=data[data_type][-1]['id']
                jsbucketDump.dump(f"{data_type}_maxid/" + "maxid_" + writetime + ".json", maxid)

                print('updated maxid=',maxid)
                sleep(1)   # 1 sec delay to insure different filenames
                i=i+200

                total_records = total_records + len(data.keys())

                if len(data.keys()) < 200 or time.time() > timeout:
                    break

            if time.time() > timeout:    
                break

        return total_records

    return pull_data(data_type)

      
