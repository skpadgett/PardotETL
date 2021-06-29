import boto3 
from boto3 import client
from os import environ
import asyncio
import json

# Pardot Task prefix
tasks_prefix = environ['PardotTaskPrefix']

# Mapping of the tasks that need to be mapped to each data type
task_mapping = {"TaskGroup1":{"tasks":[
                    {"task":"PardotPull", 
                    "data_types":[ "visitoractivities","campaigns",
                                    "lists","prospects","tagobjects","tags",
                                    "opportunities","prospectaccounts","forms",
                                    "listmemberships","visitors"]},
                    {"task":"PardotPullEmailStats",
                    "data_types":["emailstats"]}]
                    }
                    }


lambda_client = client('lambda')

def execute_tasks(task_dict, lambda_client):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(execute_task_group_async(task_dict, lambda_client))
    return 

async def execute_task_group_async(task_dict, client):
    '''
    Asynchronously executes async task collection as a group
    @param tasks (dict): collection of async task names    
    '''
    tasks = [[[task,data_type] for data_type in task_dict[task]] for task in task_dict.keys()][0]
    return await asyncio.gather(*[invoke_function(task[0], task[1],client) for task in tasks])

def print_results(task,payload):
    print(task,payload)
    return 1

async def invoke_function(task, data_type, lambda_client):
    '''
    Invokes lambda function for all the different data types
    '''
    payload = {'Metadata': {'DataType': data_type}}
    if data_type == 'visitoractivities':
        await asyncio.sleep(5)
    return print_results(task,payload)

    return await response = lambda_client.invoke(
            FunctionName=task,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload))

    print(response)

    return 


def lambda_handler(event, context):
    
    print(f'Starting Pardot Batch pull')

    execute()

def execute():
    def PardotBatch():
        for taskgroup in task_mapping.keys():
            updated_dict = {}
            for tasks in task_mapping[taskgroup]["tasks"]:
                for task in tasks.keys():
                    updated_dict[f'{tasks_prefix}-{tasks["task"]}'] = tasks['data_types']
            execute_tasks(updated_dict, lambda_client)

    PardotBatch()






###########

import boto3 
from boto3 import client
from os import environ
import asyncio
import json

# Pardot Pull Directory
pardot_pull_directory = environ['PardotPullDirectory']

# Validation Task Directory
tasks_prefix = environ['PardotTaskPrefix']

# Mapping of the tasks that need to be mapped to each data type
task_mapping = {"TaskGroup1":{"tasks":[
                    {"task":"PardotPull", 
                    "data_types":[ "visitoractivities","campaigns",
                                    "lists","prospects","tagobjects","tags",
                                    "opportunities","prospectaccounts","forms",
                                    "listmemberships","visitors"]},
                    {"task":"PardotPullEmailStats",
                    "data_types":["emailstats"]}]
                    },
                "TaskGroup2":{"tasks":[
                    {"task":"PardotPull2", 
                    "data_types":[ "visitoractivities","campaigns"]}]}
                    }


lambda_client = client('lambda')

def lambda_handler(event, context):
    
    print(f'Starting Pardot Batch pull')

    execute()

def execute_tasks(task_dict, lambda_client):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(execute_task_group_async(task_dict, lambda_client))
    return 

async def execute_task_group_async(task_dict, lambda_client):
    '''
    Asynchronously executes async task collection as a group
    @param tasks (dict): collection of async task names    
    '''
    tasks = [[[task,data_type] for data_type in task_dict[task]] for task in task_dict.keys()][0]
    return await asyncio.gather(*[invoke_function(task[0], task[1], lambda_client) for task in tasks])

async def invoke_function(task, data_type, lambda_client):
    '''
    Invokes lambda function for all the different data types
    '''
    payload = {'Metadata': {'DataType': data_type}}    

    response = lambda_client.invoke(
            FunctionName=task,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload))

    return json.load(response.get('Payload'))

def execute():
    def PardotBatch():
        for taskgroup in task_mapping.keys():
            updated_dict = {}
            for tasks in task_mapping[taskgroup]["tasks"]:
                for task in tasks.keys():
                    updated_dict[f'{tasks_prefix}-{tasks["task"]}'] = tasks['data_types']
            execute_tasks(updated_dict, lambda_client)

    PardotBatch()

      

      
