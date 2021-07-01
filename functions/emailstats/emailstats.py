# Pardot Extract - Emailstats_Campaign=Partner Success
# Note: need to change s3 folder at end - testing
#
# Plan - separate extracts by campaign, driven by separate copies
# of listemailid file in s3 directory per campaign
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
import numpy as np
from datetime import datetime
from time import sleep
import os
from smart_open import smart_open

# def emailstats_func(event, context):
def emailstats_func():
# Create class for writing to s3 bucket
    class S3csvBucket:
        def __init__(self, bucket_name):
            self.bucket = boto3.resource("s3").Bucket(bucket_name)

        def dump(self, key, obj):
            return self.bucket.Object(key=key).put(Body=(obj))
           
    # bucket_name="de-sandbox-us-east-2"        
    # listemailid bucket processing
    snapshot_date=time.strftime("%Y-%m-%d")  
    tbucket_name="de-sandbox-us-east-2"        
    csvbucket = S3csvBucket(tbucket_name)
    csvbucket2=boto3.resource("s3").Bucket(tbucket_name)
    # aws_key = os.environ['AWS_ACCESS_KEY_ID']
    # aws_secret = os.environ['AWS_SECRET_ACCESS_KEY']   
    last_modified_date = datetime(1939, 9, 1).replace(tzinfo=None)
    for file in csvbucket2.objects.filter(Prefix='listemailids/Tier_1/ListEmailIds_to_S3 for email Stats'):    
        file_date = file.last_modified.replace(tzinfo=None)
        if last_modified_date < file_date: 
           last_modified_date = file_date
    print(last_modified_date)
    key_to_download =file.key
  # path = 's3://{}:{}@{}/{}'.format(aws_key, aws_secret, tbucket_name, key_to_download)  # getting credentials from environment variables
    path='s3://{}/{}'.format(tbucket_name, key_to_download)
    
    df = pd.read_csv(smart_open(path))
    df2 = df[: -1]
    df2 = df2.rename(columns={'Pardot Listemailid Rank List Email ID':'listemailid','Pardot Listemailid Rank Campaign ID':'Campaign_ID','Pardot Campaign Name':'Campaign_Name','Pardot Listemailid Rank Max Created At':'Max_Created_At'})
        
    df2['listemailid'] = df2['listemailid'].astype('Int64')
    df2['Campaign_ID'] = df2['Campaign_ID'].astype('Int64')

# Now have clean DF ready for iteration


#Create PardotAPI class instance
    p=PardotAPI(email='segment_integrations@discoveryed.com',user_key='f5a1dc61d079e35d9a2066a4a8498c32',sf_consumer_key='3MVG9IHf89I1t8hpom1l0QzbTANHH.MOASIZ3yPPhu6hFI_uipXiYi7ku85yMWg_2gXxu5bzAyWCevOHp4jrf',sf_consumer_secret='BC6C2AA5D41DC0EBBBB3376CD329E92E66BD8D85DF85F9FAB9F22E86D857A29E',sf_refresh_token='5Aep8618yVsldz6rZPMv4ouelrGoRNAfdaLSjo3.ILw4jdNGemJVI_MNYFoNkN9g39GUXqUEgzw3ylEVMjxAU3X',business_unit_id='0Uv4P000000TNB0SAO',version=4)

#Get email stats for each list_email_id
    stats_data=pd.DataFrame()
    for row in zip(df2['listemailid'],df2['Campaign_ID'],df2['Campaign_Name'],df2['Max_Created_At']):
        j=row[0]
        #results=p.emails.stats(list_email_id=j)
        results=pd.DataFrame.from_dict(p.emails.stats(list_email_id=j))
        #For reference DataFrame.from_dict(data, orient='index', dtype=None, columns='list_email_id')
        #Add list_email_id to result DataFrame
        results['list_email_id']=j
        #Add name, created_at, and subject from email to result DataFrame
        mail=pd.DataFrame.from_dict(p.emails.read(email_id=j))
        results.loc['name']= ['ok',mail.loc['name','email'],j]
        results.loc['created_at']= ['ok',mail.loc['created_at','email'],j]
        results.loc['subject']=['ok',mail.loc['subject','email'],j]
        results.loc['sent_date']=['ok',row[3],j]
        results.loc['campaign_id']=['ok',row[1],j]
        results.loc['campaign_name']=['ok',row[2],j]
        results.loc['snapshot_date']=['ok',snapshot_date,j]
        stats_data=results.append(stats_data,sort=True)
    #print(results)
    #Drop @attributes column
    stats_data.drop(columns=['@attributes'])
    #create column containing index values
    stats_data['colheader'] = stats_data.index
    #pivot dataframe
    stats_data2=stats_data.pivot(index='list_email_id',columns='colheader',values='stats')    
    #Convert dataframe to csv format 
    csvobject=stats_data2.to_csv(index=True)	
    writetime=time.strftime("%Y%m%d-%H%M%S")
    try:
        #csvbucket.dump("emailstats/" + "emailstats_" + writetime + ".csv", csvobject) 
        csvbucket.dump("emailstats/" + "emailstats_" + writetime + ".csv", csvobject)
        print ('s3 success for emailstats_Tier_1 file @ ',writetime)
    except:
        print('Error posting to s3 for emailstats_Tier_1 file ')


    #import code    
    #code.interact(local=locals())
if __name__ == '__main__':
    emailstats_func()        
