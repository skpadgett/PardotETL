# Pardot Extract Historical Records via Export API - Visitors
#
# Notes on 4/15/2021 Execution:
# Getting error in 
#for key in fltr_key: str1=attrs[key]
#auth='Bearer'+' '+str1
# So manually constructed auth and then cut and pasted commands into python prompt:
# Could not extract > 12 Months, so broke into two iterations 7/1/2019-4/15/2020; and 4/15/2020-4/15/2021
# Prep Steps
# 1. Create class for writing to s3 bucket
# 2. Populate PardotAPI object
# 3. Execute Pardot API wrapper call to refresh token
# 4. Insert updated token into headers parameter
# 5. Populate data parameter 
# 6. API call to create extract request
#
# Following Done in separate job to avoid iteration over request status
# Not Doing - 7. Iteratively  check status of extract request for up to xx minutes
# 8. Iteratively process list of filenames created (still residing within Pardot)
# 9.       - Retrieve Files and post to s3 bucket in test/visitors_hist/visitors/
###########################################################

from pypardot.client import PardotAPI		  #Pardot API Wrapper	
import pandas as pd							  # Dataframe and data transformation functions   #included in layer

import time
import json
from datetime import datetime
from time import sleep
import sys
import requests


		
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

#############################################
#Data parameter settings for the five different versions (only 1 used in this script, will have 5 versions)
#visitorActivity
#
#VA_data = '{"object": "visitorActivity","procedure": {"name": "filter_by_created_at","arguments": {"created_after": "2020-11-25 00:00:01","created_before": "2020-11-30 24:59:59"}}}'
#
#Prospect
#
#P_data = '{"object": "prospect","procedure": {"name": "filter_by_updated_at","arguments": {"updated_after": "2020-11-25 00:00:01","updated_before": "2020-11-30 24:59:59"}}}'
#
#Prospectaccount
#
#PA_data = '{"object": "prospectaccount","procedure": {"name": "filter_by_prospect_updated_at","arguments": {"prospect_updated_after": "2020-11-25 00:00:01","prospect_updated_before": "2020-11-30 24:59:59"}}}'	
#
#Visitor
#
#V_data = '{"object": "visitor","procedure": {"name": "filter_by_updated_at","arguments": {"updated_after": "2020-06-01 00:00:01","updated_before": "2020-04-15 00:00:01"}}}'
#
#Listmembership
#
#L_data = '{"object": "listmembership","procedure": {"name": "filter_by_updated_at","arguments": {"updated_after": "2020-11-25 00:00:01","updated_before": "2020-11-30 24:59:59"}}}'
#
#####################################################
#  Multistep Process
#  1. Create Request
#  2. Read Request for Status & Results if complete
#  3. If Status not complete, repeat after 30 second delay, up to 
#     5 ? times
#  4. Once complete, iterate through response, retrieving files and posting
#     to s3 bucket.
#
#
#First Request - Visitors
#Header to use:
V_data = '{"object": "visitor","procedure": {"name": "filter_by_updated_at","arguments": {"updated_after": "2019-07-01 00:00:01","updated_before": "2020-07-01 00:00:01"}}}'
#Create Extract Request
response = requests.post('https://pi.pardot.com/api/export/version/4/do/create', headers=headers, params=params, data=V_data)
# Get assigned export id
response_dict=json.loads(response.text)
exportid=response_dict['export']['id']
print('Vistor Export id =',exportid)

#Second Request - Prospects
#Header to use:
V_data = '{"object": "prospect","procedure": {"name": "filter_by_updated_at","arguments": {"updated_after": "2019-07-01 00:00:01","updated_before": "2020-07-01 00:00:01"}}}'
#Create Extract Request
response = requests.post('https://pi.pardot.com/api/export/version/4/do/create', headers=headers, params=params, data=V_data)
# Get assigned export id
response_dict=json.loads(response.text)
exportid=response_dict['export']['id']
print('Prospects Export id =',exportid)

#Third Request - Prospect Accounts
#Header to use:
V_data = '{"object": "prospectaccount","procedure": {"name": "filter_by_prospect_updated_at","arguments": {"prospect_updated_after": "2019-07-01 00:00:01","prospect_updated_before": "2020-07-01 00:00:01"}}}'	
#Create Extract Request
response = requests.post('https://pi.pardot.com/api/export/version/4/do/create', headers=headers, params=params, data=V_data)
# Get assigned export id
response_dict=json.loads(response.text)
exportid=response_dict['export']['id']
print('Prospect Accounts Export id =',exportid)

#Fourth Request - Listmembership
#Header to use:
V_data = '{"object": "listmembership","procedure": {"name": "filter_by_updated_at","arguments": {"updated_after": "2019-07-01 00:00:01","updated_before": "2020-07-01 00:00:01"}}}'
#Create Extract Request
response = requests.post('https://pi.pardot.com/api/export/version/4/do/create', headers=headers, params=params, data=V_data)
# Get assigned export id
response_dict=json.loads(response.text)
exportid=response_dict['export']['id']
print('Listmembership Export id =',exportid)

#Fifth Request - VisitorActivity
#Header to use:
V_data = '{"object": "visitorActivity","procedure": {"name": "filter_by_created_at","arguments": {"created_after": "2019-07-01 00:00:01","created_before": "2020-07-01 00:00:01"}}}'
#Create Extract Request
response = requests.post('https://pi.pardot.com/api/export/version/4/do/create', headers=headers, params=params, data=V_data)
# Get assigned export id
response_dict=json.loads(response.text)
exportid=response_dict['export']['id']
print('VisitorActivity Export id =',exportid)
##########################################################
#############################################################
#   Date range - after 2020-07-01 before 2021-07-01
#########################################################
##########################################################









		
