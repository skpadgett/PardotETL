# Pardot Extract - Accounts (all time)
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

def accounts_func(event, context):
# Create class for writing to s3 bucket
    class S3JsonBucket:
        def __init__(self, bucket_name):
            self.bucket = boto3.resource("s3").Bucket(bucket_name)

        def load(self, key):
            return json.load(self.bucket.Object(key=key).get()["Body"])

        def dump(self, key, obj):
            return self.bucket.Object(key=key).put(Body=json.dumps(obj))
       
        def writejsn(self, key, obj):
            return self.bucket.Object(key=key).put(Body=obj)       

#Create PardotAPI class instance
    p=PardotAPI(email='segment_integrations@discoveryed.com',user_key='f5a1dc61d079e35d9a2066a4a8498c32',sf_consumer_key='3MVG9IHf89I1t8hpom1l0QzbTANHH.MOASIZ3yPPhu6hFI_uipXiYi7ku85yMWg_2gXxu5bzAyWCevOHp4jrf',sf_consumer_secret='BC6C2AA5D41DC0EBBBB3376CD329E92E66BD8D85DF85F9FAB9F22E86D857A29E',sf_refresh_token='5Aep8618yVsldz6rZPMv4ouelrGoRNAfdaLSjo3.ILw4jdNGemJVI_MNYFoNkN9g39GUXqUEgzw3ylEVMjxAU3X',business_unit_id='0Uv4P000000TNB0SAO',version=4)

    jsbucket = S3JsonBucket("de-sandbox-us-east-2")

    data=p.accounts.read(format='json')
    writetime=time.strftime("%Y%m%d-%H%M%S")
#    json_object = json.dumps(data["account"])
#    finalfinalstr=('{"account":['+json_object+']}')
    jsbucket.dump("accounts/" + "accounts_" + writetime + ".json", data)
	  
if __name__ == '__main__':
    accounts_func()