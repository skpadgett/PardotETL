import boto3
from boto3 import client
from os import environ
import asyncio
import json

# Pardot Task prefix
TASKS_PREFIX = environ.get("PardotTaskPrefix")

# Mapping of TaskGroups to Tasks to DataTypes
# As of right now there is only one task group, but if some kind of postprocesing was required, this could handle it
#   with a new group
task_mapping = {
    "TaskGroup1": {
        "tasks": [
            {
                "task": "PardotPull",
                "data_types": [
                    "visitoractivities",
                    "campaigns",
                    "lists",
                    "prospects",
                    "tagobjects",
                    "tags",
                    "opportunities",
                    "prospectaccounts",
                    "forms",
                    "listmemberships",
                    "visitors",
                ],
            },
            {"task": "PardotPullEmailStats", "data_types": ["emailstats"]},
        ]
    }
}


lambda_client = client("lambda")


def execute_tasks(task_dict, lambda_client):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(execute_task_group_async(task_dict, lambda_client))
    return


async def execute_task_group_async(task_dict, client):
    """
    Asynchronously executes task / data_type collection as a group
    Have to reformat the dictionary into a list to allow asyncio gather to work
    """
    tasks = [
        [[task, data_type] for data_type in task_dict[task]]
        for task in task_dict.keys()
    ][0]
    return await asyncio.gather(
        *[invoke_function(task[0], task[1], client) for task in tasks]
    )


async def invoke_function(task, data_type, lambda_client):
    """
    Invokes lambda function for all for a specific task, for a specific type of data
    """
    payload = {"Metadata": {"DataType": data_type}}

    response = lambda_client.invoke(
        FunctionName=task, InvocationType="RequestResponse", Payload=json.dumps(payload)
    )

    return response


def lambda_handler(event, context):

    print(f"Starting Pardot Batch pull")

    execute()


def execute():
    def PardotBatch():
        for taskgroup in task_mapping.keys():
            updated_dict = {}
            for tasks in task_mapping[taskgroup]["tasks"]:
                for task in tasks.keys():
                    # Update the task name to match the lambda name (ie. pardot-us-east-2-prod.. might get added)
                    updated_dict[f'{TASKS_PREFIX}-{tasks["task"]}'] = tasks[
                        "data_types"
                    ]
            execute_tasks(updated_dict, lambda_client)

    PardotBatch()
