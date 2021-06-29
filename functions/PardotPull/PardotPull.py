from pypardot.client import PardotAPI		  #Pardot API Wrapper	
import pandas as pd							  # Dataframe and data transformation functions   #included in layer
import boto3                                  # Writing files to S3
import time
import json
from datetime import datetime
from time import sleep
from os import environ

# Gather environmental variables
email = environ.get('pardotEmail')
user_key = environ.get('pardotUserKey')
sf_consumer_key = environ.get('pardotSfConsumerKey')
sf_consumer_secret = environ.get('pardotSfConsumerSecret')
sf_refresh_token = environ.get('pardotSfRefreshToken')
business_unit_id = environ.get('pardotSfBusinessUnitID')
version = environ.get('pardotVersion')
bucket = environ.get('s3FileStore')

def lambda_handler(event, context):
    
    start_time = time.now()

    data_type = event['Metadata'].get('DataType')

    print(f'Starting Pardot pull for {data_type}')

    execute(data_type)

    end_time = time.now()
    
    print(f"""Finished Pardot pull for {data_type}. 
    Process took {end_time-start_time} seconds to pull"")

    return

def execute(data_type):
    # Create class for writing to s3 bucket
    def pull_data(data_type):
        class S3JsonBucket:
            def __init__(self, bucket_name):
                self.bucket = boto3.resource("s3").Bucket(bucket_name)

            def load(self, key):
                return json.load(self.bucket.Object(key=key).get()["Body"])

            def dump(self, key, obj):
                return self.bucket.Object(key=key).put(Body=json.dumps(obj))

        #Instantiate Pardot Client
        p=PardotAPI(email=email,user_key=user_key,sf_consumer_key=sf_consumer_key,sf_consumer_secret=sf_consumer_secret,sf_refresh_token=sf_refresh_token,business_unit_id=business_unit_id,version=version)

        maxid=0
        i=0
        jsbucket = S3JsonBucket(bucket)

        if data_type == 'visitoractivities':
            data_client = p.visitoractivities

        elif data_type == 'campaigns':
            data_client = p.visitoractivities

        elif data_type == 'lists':
            data_client = p.visitoractivities

        elif data_type == 'prospects':
            data_client = p.visitoractivities

        elif data_type == 'tagobjects':
            data_client = p.visitoractivities

        elif data_type == 'tags':
            data_client = p.visitoractivities

        else :
            raise Exception (f'{data_type} is not a valid selection!')
            return



        while i <=data_client.query(created_after='yesterday',created_before='today')['total_results'] -1: 
            data=data_client.query(format='json',sort_by='id',created_after='yesterday',created_before='today',id_greater_than=maxid)
            maxid=data[data_type][-1]['id']
            writetime=time.strftime("%Y%m%d-%H%M%S") 
            file_name = f"test/{data_type}/{data_type}_{writetime}.json"
            jsbucket.dump(file_name, data)
            print(f'Sent {file_name} to {bucket} with {len(data.keys())} records')
            sleep(1)   # 1 sec delay to insure different filenames
            i=i+200

    pull_data(data_type)

      
