# Pardot Extract - prospects created_after='yesterday',created_before='today'
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

def prospects_func(event, context):
# Create class for writing to s3 bucket
    class S3JsonBucket:
        def __init__(self, bucket_name):
            self.bucket = boto3.resource("s3").Bucket(bucket_name)

        def load(self, key):
            return json.load(self.bucket.Object(key=key).get()["Body"])

        def dump(self, key, obj):
            return self.bucket.Object(key=key).put(Body=json.dumps(obj))

#Create PardotAPI class instance
    p=PardotAPI(email='segment_integrations@discoveryed.com',user_key='f5a1dc61d079e35d9a2066a4a8498c32',sf_consumer_key='3MVG9IHf89I1t8hpom1l0QzbTANHH.MOASIZ3yPPhu6hFI_uipXiYi7ku85yMWg_2gXxu5bzAyWCevOHp4jrf',sf_consumer_secret='BC6C2AA5D41DC0EBBBB3376CD329E92E66BD8D85DF85F9FAB9F22E86D857A29E',sf_refresh_token='5Aep8618yVsldz6rZPMv4ouelrGoRNAfdaLSjo3.ILw4jdNGemJVI_MNYFoNkN9g39GUXqUEgzw3ylEVMjxAU3X',business_unit_id='0Uv4P000000TNB0SAO',version=4)
##############################################
# Iterative retrieval of 200 record blocks (api constraint). 
# Write file to s3 with datetime suffix for each 200 block iteration
#################################################

    maxid=0
    i=0
    jsbucket = S3JsonBucket("de-sandbox-us-east-2")

    while i <=p.prospects.query(created_after='yesterday',created_before='today')['total_results'] -1: 
        data=p.prospects.query(format='json',sort_by='id',created_after='yesterday',created_before='today',id_greater_than=maxid)
        maxid=data['prospect'][-1]['id']
        writetime=time.strftime("%Y%m%d-%H%M%S") 
        jsbucket.dump("prospects/" + "prospects_" + writetime + ".json", data)
        sleep(1)   # 1 sec delay to insure different filenames
        i=i+200
        
if __name__ == '__main__':
    prospects_func()


