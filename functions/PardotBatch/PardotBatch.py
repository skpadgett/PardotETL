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


lambda_client = client("lambda")


def lambda_handler(event, context):

    print(f"Starting Pardot Batch pull")

    for task, list_data_type in {
        "PardotPull": [
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
        "PardotPullEmailStats": ["emailstats"],
    }.items():
        for data_type in list_data_type:
            lambda_client.invoke(
                FunctionName=task,
                InvocationType="Event",
                Payload=json.dumps({"Metadata": {"DataType": data_type}}),
            )
