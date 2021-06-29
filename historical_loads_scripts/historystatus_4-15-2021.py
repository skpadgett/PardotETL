#
# Line below worked in this job
#
# for key in fltr_key: str1=attrs[key]
#auth='Bearer'+' '+str1
from pypardot.client2 import PardotAPI        #Pardot API Wrapper   
import pandas as pd                           # Dataframe and data transformation functions   #included in layer
import boto3                                  # Writing files to S3
import time
import json
from datetime import datetime
from time import sleep
import sys
import requests
# Create class for writing to s3 bucket
class S3csvBucket:
    def __init__(self, bucket_name):
        self.bucket = boto3.resource("s3").Bucket(bucket_name)

    def dump(self, key, obj):
        return self.bucket.Object(key=key).put(Body=(obj))

csvbucket = S3csvBucket("de-sandbox-us-east-2")     
# Get class PardotAPI       
p=PardotAPI(email='segment_integrations@discoveryed.com',user_key='f5a1dc61d079e35d9a2066a4a8498c32',sf_consumer_key='3MVG9IHf89I1t8hpom1l0QzbTANHH.MOASIZ3yPPhu6hFI_uipXiYi7ku85yMWg_2gXxu5bzAyWCevOHp4jrf',sf_consumer_secret='BC6C2AA5D41DC0EBBBB3376CD329E92E66BD8D85DF85F9FAB9F22E86D857A29E',sf_refresh_token='5Aep8618yVsldz6rZPMv4ouelrGoRNAfdaLSjo3.ILw4jdNGemJVI_MNYFoNkN9g39GUXqUEgzw3ylEVMjxAU3X',business_unit_id='0Uv4P000000TNB0SAO',version=4)

#Execute Pardot Wrapper call to get token refreshed
campaigns=p.campaigns.query()  #which wrapper call is irrelevant, using campaigns because of low data volume
attrs = vars(p)
#get token from class object
fltr_key=['sftoken']
str1=" "
for key in fltr_key: str1=attrs[key]
auth='Bearer'+' '+str1

# Insert token in headers parameter
headers= {'content-type': 'application/json','Authorization': auth, 'Pardot-Business-Unit-Id': '0Uv4P000000TNB0SAO'}
params = (('format', 'json'),)

#Request #1
exportid=731
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=733
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=735
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=737
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=739
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=741
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=743
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=745
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=747
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

#Request #1
exportid=749
httpvar='https://pi.pardot.com/api/export/version/4/do/read/id/'+str(exportid)
#Read Request
get_status = requests.get(httpvar, headers=headers, params=params)
exprt_status_dict=json.loads(get_status.text)
exprt_status=exprt_status_dict['export']['state']
print('export id = ',exportid,exprt_status)

