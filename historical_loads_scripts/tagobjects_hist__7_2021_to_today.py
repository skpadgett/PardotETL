# Pardot Extract - tagobjects created_after='2020-07-01',created_before='today',id_greater_than=maxid
#                  of maxid to s3 folder.  Maxid logic is to support multiple executions of 
#                  this 
#
# For local execution of this script, requires temporary access credentials entered
# at command line. See...
#    https://aws.amazon.com/blogs/security/aws-single-sign-on-now-enables-command-line-interface-access-for-aws-accounts-using-corporate-credentials/
#    Navigation path used for history loads documented at google doc (needs to be shared)
#    https://docs.google.com/document/d/188jLokq0A5B9YNcp_O-frnoWpLkZu6eLau30mVd6G94/edit
#
#
from pypardot.client import PardotAPI		  #Pardot API Wrapper	
import pandas as pd							  # Dataframe and data transformation functions   #included in layer
import boto3                                  # Writing files to S3
import time
import json
from datetime import datetime
from time import sleep
import os
from smart_open import smart_open
def tagobjects_func():
#def tagobjects_func(event, context):
# Create class for writing to s3 bucket
    class S3JsonBucket:
        def __init__(self, bucket_name):
            self.bucket = boto3.resource("s3").Bucket(bucket_name)

        def load(self, key):
            return json.load(self.bucket.Object(key=key).get()["Body"])

        def dump(self, key, obj):
            return self.bucket.Object(key=key).put(Body=json.dumps(obj))

    v_bucketname="de-sandbox-us-east-2"
    jsbucket = S3JsonBucket(v_bucketname)
    jsbucket_2=boto3.resource("s3").Bucket(v_bucketname)    
#
# tagobjects_maxid retrieval

    last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None)
    
    #Iterate through files to get the latest
    for file in jsbucket_2.objects.filter(Prefix='tagobject_maxid/ta_maxid'):    
        file_date = file.last_modified.replace(tzinfo=None)
        if last_modified_date < file_date: 
           last_modified_date = file_date
        # delete s3 files except for latest   
        else: jsbucket_2.delete_objects( Delete={'Objects': [{'Key': file.key},],})
           
    print('last modified date=',last_modified_date)
    key_to_download =file.key
    # Initialize maxid from retrieved file
    path='s3://{}/{}'.format(v_bucketname, key_to_download)
    maxid = json.load(smart_open(path))
    print('initial maxid=',maxid)
   

#Create PardotAPI class instance
    p=PardotAPI(email='segment_integrations@discoveryed.com',user_key='f5a1dc61d079e35d9a2066a4a8498c32',sf_consumer_key='3MVG9IHf89I1t8hpom1l0QzbTANHH.MOASIZ3yPPhu6hFI_uipXiYi7ku85yMWg_2gXxu5bzAyWCevOHp4jrf',sf_consumer_secret='BC6C2AA5D41DC0EBBBB3376CD329E92E66BD8D85DF85F9FAB9F22E86D857A29E',sf_refresh_token='5Aep8618yVsldz6rZPMv4ouelrGoRNAfdaLSjo3.ILw4jdNGemJVI_MNYFoNkN9g39GUXqUEgzw3ylEVMjxAU3X',business_unit_id='0Uv4P000000TNB0SAO',version=4)

    i=0
    while i <=p.tagobjects.query(created_after='2020-07-01',created_before='today',id_greater_than=maxid)['total_results'] -1: 
        data=p.tagobjects.query(format='json',sort_by='id',created_after='2020-07-01',created_before='today',id_greater_than=maxid)
        maxid=data['tagObject'][-1]['id']
        writetime=time.strftime("%Y%m%d-%H%M%S") 
        jsbucket.dump("tagobjects/" + "tagobjects_" + writetime + ".json", data)
        # write current maxid to S3 - If lambda times out, next execution will pickup with this value of maxid.
     
        data2=maxid
        jsbucket.dump("tagobject_maxid/" + "ta_maxid_" + writetime + ".json", data2)
        print('updated maxid=',maxid)
        sleep(1)   # 1 sec delay to insure different filenames
        i=i+200

if __name__ == '__main__':
    tagobjects_func(1,2)   #remove parameters for lambda