import os
import io
import time
import json
import datetime as dt
import typing
import requests

import pandas as pd
import boto3
import snowflake.connector
from pypardot.client import PardotAPI, PardotAPIError
from xml.etree import ElementTree

# Gather environmental variables
PARDOT_EMAIL = os.environ["pardotEmail"]
PARDOT_USER_KEY = os.environ["pardotUserKey"]
PARDOT_SF_CONSUMER_KEY = os.environ["pardotSfConsumerKey"]
PARDOT_SF_CONSUMER_SECRET = os.environ["pardotSfConsumerSecret"]
PARDOT_SF_REFRESH_TOKEN = os.environ["pardotSfRefreshToken"]
PARDOT_BUSINESS_UNIT_ID = os.environ["pardotSfBusinessUnitID"]
PARDOT_API_VERSION = os.environ["pardotVersion"]
PARDOT_MAX_RESULT_COUNT = 200
AWS_NAME_BUCKET = os.environ["AWS_NAME_BUCKET"]
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-2")
AWS_SNS_TOPIC_ARN_EMAIL_NOTIFICATION = (
    "arn:aws:sns:us-east-1:768217030320:test-topic-email-notification"
)
SNOWFLAKE_ACCOUNT_IDENTIFIER = os.environ["SNOWFLAKE_ACCOUNT_IDENTIFIER"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASS = os.environ["SNOWFLAKE_PASS"]

TIMEOUT_SECONDS = 60 * 60 * 6  # <-- 6 Hour timeout
PARDOT_URL_API = f"https://pi.pardot.com/api/export/version/{PARDOT_API_VERSION}"
FORMAT_WRITETIME = r"%Y%m%d-%H%M%S"
FORMAT_DATETIME_API = r"%Y-%m-%d:%H:%M:%S"
# ^ Depricated, isoformat only works
INT_MAX_BUFFER = 1000
# ^ Limit number of records of output file for segmented export to this number

DATE_VERY_EARLY = dt.date(1900, 1, 1)

DICT_CONVERSION_PURAL = {
    "Account": "accounts",
    "EmailClick": "emailclicks",
    "Email": "emails",
    "VisitorActivity": "visitoractivities",
    "Campaign": "campaigns",
    "List": "lists",
    "Prospect": "prospects",
    "TagObject": "tagobjects",
    "Tag": "tags",
    "Opportunity": "opportunities",
    "ProspectAccounts": "prospectaccounts",
    "Form": "forms",
    "ListMembership": "listmemberships",
    "Visitor": "visitors",
    "ProspectAccount": "prospectaccounts",
}

DICT_MAP_SNOWFLAKE_TABLE = {"Opportunity": "Opportunities"}
SET_DATA_TYPE_CREATED = {
    # This set is for the data types that do not have a "updated_after" filter,
    # and must be queried usng the "created_after" filter
    "EmailClick",
    "TagObject",
}

global_num_calls_api: int = 0


# TODO: Add number of API calls made after process to SNS topic


def get_client_pardot() -> PardotAPI:
    try:
        p = PardotAPI(
            email=PARDOT_EMAIL,
            user_key=PARDOT_USER_KEY,
            sf_consumer_key=PARDOT_SF_CONSUMER_KEY,
            sf_consumer_secret=PARDOT_SF_CONSUMER_SECRET,
            sf_refresh_token=PARDOT_SF_REFRESH_TOKEN,
            business_unit_id=PARDOT_BUSINESS_UNIT_ID,
            version=PARDOT_API_VERSION,
        )

        if not p.authenticate():
            p.campaigns.query(limit=1)
            # ^ Must call query to authenticate if authenticatation is unsuccessful

    except PardotAPIError as err:
        if err.message in [
            "Error #184: access_token is invalid, unknown, or malformed",
            "Error #122: Daily API rate limit met",
        ]:
            get_session_boto().client("sns").publish(
                TargetArn="arn:aws:sns:us-east-1:768217030320:test-topic-email-notification",
                Message=f"PARDOT ERROR: {err.message}",
                # Message=json.dumps({"default": json.dumps(message)}),
                MessageStructure="string",
            )
        raise

    assert (
        p.sftoken != "dummy"
    ), "Pardot authentication still unsuccessful after querying"

    return p


def get_session_boto() -> boto3.Session:
    return boto3.Session(
        region_name=AWS_DEFAULT_REGION
    )


def get_date_start_snowflake(data_type,tagobject_type=None) -> dt.date:
    "Return the latest updated_at date in Snowflake for the specified data type"

    ctx = get_client_snowflake()

    if data_type in ["Campaign"]:
        # Return very early date if its not supported
        return DATE_VERY_EARLY

    _data_type = DICT_MAP_SNOWFLAKE_TABLE.get(data_type, data_type)
    # ^ Some data types need to be changed for the snowflake table name
    name_table_snowflake = f"PARDOT_{_data_type.upper()}"
    name_field_table = "UPDATED_AT"
    if data_type in {"EmailClick"}:
        name_field_table = "CREATED_AT"

    if tagobject_type!=None:
        type_filter = f" and type={tagobject_type !r}"
    else:
        type_filter = ''

    cs = ctx.cursor()
    try:
        # Check if the table exists in SnowFlake
        cs.execute(f"SHOW TABLES LIKE '{name_table_snowflake}' IN DEV_DATA_VAULT.Stage")
        table_check = cs.fetchone()

        if table_check is None:
            raise Exception(f'{name_table_snowflake} doesn''t exist in SnowFlake. Add table & snowpipe')
        else:
            cs.execute(
                f"""SELECT max({name_field_table})
                FROM {name_table_snowflake}
                WHERE {name_field_table} IS NOT null{type_filter}"""
            )
            one_row = cs.fetchone()
            if one_row is None:
                result = None
            else:
                result = one_row[0]

    finally:
        cs.close()
    ctx.close()

    if result is None:
        # Return a very early datetime
        return DATE_VERY_EARLY
    elif isinstance(result, str):
        return dt.datetime.strptime(result, "%Y-%m-%d:%H:%M:%S").date
    elif isinstance(result, dt.date):
        return result
    elif isinstance(result, dt.datetime):
        return result.date
    else:
        raise Exception(
            f"Unknown result: {result !r} from row {one_row !r} for data type {data_type !r}"
        )

    ## Placeholder for testing
    # return DATE_VERY_EARLY
    # return dt.date.today() - dt.timedelta(days=1)


def get_client_snowflake():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASS,
        account=SNOWFLAKE_ACCOUNT_IDENTIFIER,
        # authenticator="externalbrowser",
        warehouse="GENERAL_COMPUTE_WH",
        database="DEV_DATA_VAULT",
        schema="STAGE",
    )


def export_bulk(data_type: str):
    """Bulked export (which is prefered), wherein a job is queued and executed
    server-side that will give result URLs"""

    global global_num_calls_api

    assert (
        data_type in DICT_CONVERSION_PURAL
    ), f"{data_type} not defined in conversion map (you may need to add this in the code)"

    p = get_client_pardot()

    headers = {
        "content-type": "application/json",
        "Authorization": f"Bearer {p.sftoken}",
        "Pardot-Business-Unit-Id": PARDOT_BUSINESS_UNIT_ID,
    }

    date_start = get_date_start_snowflake(data_type)

    response_create = requests.post(
        f"{PARDOT_URL_API}/do/create",
        headers=headers,
        params=(("format", "json"),),
        data=json.dumps(
            {
                "object": data_type,
                "procedure": {
                    "name": "filter_by_updated_at",
                    "arguments": {
                        "updated_after": (
                            dt.date.today() - dt.timedelta(days=1)
                        ).isoformat(),
                        **(
                            dict(created_after=date_start.isoformat())
                            if data_type in SET_DATA_TYPE_CREATED
                            else {}
                        ),
                        # "updated_before": (
                        #     dt.date.today() - dt.timedelta(days=0)
                        # ).isoformat(),  # "2021-01-02 00:00:01",
                    },
                },
            }
        ),
    )
    global_num_calls_api += 1

    assert (
        response_create.status_code == 200
    ), f"Status code for response is {response_create.status_code}\nText: {response_create.text !r}"

    print(f"Bulk export request made for data type {data_type !r}")

    time_start = time.time()
    i = 0
    SECONDS_SLEEP = 60
    while time.time() < (time_start + TIMEOUT_SECONDS):
        i += 1

        try:
            response_status = requests.get(
                f"{PARDOT_URL_API}/do/read/id/{response_create.json()['export']['id']}",
                headers=headers,
                params=(("format", "json"),),
                # data=json.dumps(
                #     {
                #         "object": data_type,
                #         "procedure": {
                #             "name": "filter_by_updated_at",
                #             "arguments": {},
                #         },
                #     }
                # ),
            )
        except KeyError as e:
            print(response_create.json())
            raise e
        finally:
            global_num_calls_api += 1

        print('Check for response status issue (expect "[export][state]")',response_status.json())

        state_export: str = response_status.json()["export"]["state"]
        if state_export in ["Waiting", "Processing"]:
            time.sleep(SECONDS_SLEEP)
            #TODO, remove this it's useless
            if i % 6 == 0:
                print(
                    f"STATUS: {state_export !r}... ({i * SECONDS_SLEEP} seconds wait time elapsed)"
                )
        elif state_export == "Complete":
            assert "resultRefs" in response_status.json()["export"]
            break
        else:
            raise Exception(f"Export failed with state {state_export !r}")

    list_url_read: typing.List[str] = response_status.json()["export"]["resultRefs"]

    session_aws = get_session_boto()
    bucket_destination = session_aws.resource("s3").Bucket(AWS_NAME_BUCKET)

    for url_read in list_url_read:
        response_data = requests.get(url_read, headers=headers)
        global_num_calls_api += 1

        file_name = (
            f"{data_type}/{data_type}_bulk_{time.strftime(FORMAT_WRITETIME)}.csv"
        )
        bucket_destination.Object(key=file_name).put(Body=response_data.text)

        _num_rows: int = response_data.text.count("\n")
        print(f"Sent {file_name} to {AWS_NAME_BUCKET} with {_num_rows} records")


def helper_to_camelCase(s: str) -> str:
    "Return the same string with the first letter lowercase"
    return str(s[0].lower() + s[1:])


def export_segmented(data_type: str) -> int:
    "API export through regular, segmented API calls"

    assert (
        data_type in DICT_CONVERSION_PURAL
    ), f"{data_type} not defined in conversion map (you may need to add this in the code)"

    global global_num_calls_api

    p = get_client_pardot()

    try:
        data_client = getattr(p, DICT_CONVERSION_PURAL[data_type])
    except AttributeError:
        raise Exception(f"{data_type} is not a valid selection!")

    date_start = get_date_start_snowflake(data_type)

    session_aws = get_session_boto()
    bucket_destination = session_aws.resource("s3").Bucket(AWS_NAME_BUCKET)

    def _export_segmented_upload_helper(data: typing.List[dict]):
        "Transform list of dicts to CSV format, uploads to S3 bucket"

        assert isinstance(data, list)

        if len(data) == 0:
            return

        buffer = io.StringIO()
        pd.DataFrame(data).to_csv(buffer, index=False)

        file_name = (
            f"{data_type}/{data_type}_api_{time.strftime(FORMAT_WRITETIME)}.csv"
        )
        bucket_destination.Object(key=file_name).put(Body=buffer.getvalue())

        print(f"Wrote file {file_name} with {len(data)} rows")

    if not hasattr(data_client, "query"):
        # If the data_client doesn't have a query method, try the "read" method

        data = data_client.read()[helper_to_camelCase(data_type)]
        global_num_calls_api += 1

        if isinstance(data, dict):
            # Assume only one record

            _export_segmented_upload_helper([data])
            int_total_results_cumulative = 1

        elif isinstance(data, list):
            # Assume mulitiple records

            _export_segmented_upload_helper(data)
            int_total_results_cumulative = len(data)

    else:
        # Data client has query method, proceed

        time_start: int = time.time()
        id_max: int = 0
        int_total_results_cumulative: int = 0
        list_buffer_results: typing.List[dict] = []
        while True:
            # ^ Yes, this is dangerous, but its necessary for allowing more information

            time.sleep(1)

            data_raw = data_client.query(
                format="json",
                sort_by="id",
                id_greater_than=id_max,
                updated_after=date_start.isoformat(),
                **(
                    dict(created_after=date_start.isoformat())
                    if data_type in SET_DATA_TYPE_CREATED
                    else {}
                ),
            )

            global_num_calls_api += 1

            list_records = data_raw[helper_to_camelCase(data_type)]
            #                       ^ Key is camelCase
            _total_results = int(data_raw["total_results"])

            if _total_results == 0:
                break

            id_max = int(list_records[-1]["id"])

            if _total_results > int_total_results_cumulative:
                int_total_results_cumulative = _total_results

            list_buffer_results.extend(list_records)

            if len(list_buffer_results) >= INT_MAX_BUFFER:
                _export_segmented_upload_helper(list_buffer_results)
                list_buffer_results.clear()

            if _total_results < PARDOT_MAX_RESULT_COUNT:
                # Reached end of results, break
                break

            if time.time() > (time_start + TIMEOUT_SECONDS):
                raise Exception(
                    f"Timeout ({TIMEOUT_SECONDS} seconds) reached for data type {data_type !r}"
                )

        # Flush remaining results (if any)
        _export_segmented_upload_helper(list_buffer_results)

    print(
        f"Total results written for data type {data_type !r}: {int_total_results_cumulative}"
    )

    return int_total_results_cumulative

def export_tagobject_segmented(tagobject_type,data_type="TagObject"):
    "API export through regular, segmented API calls"

    global global_num_calls_api

    p = get_client_pardot()

    try:
        data_client = getattr(p, DICT_CONVERSION_PURAL[data_type])
    except AttributeError:
        raise Exception(f"{data_type} is not a valid selection!")

    date_start = get_date_start_snowflake(data_type,tagobject_type=tagobject_type)

    session_aws = get_session_boto()
    bucket_destination = session_aws.resource("s3").Bucket(AWS_NAME_BUCKET)

    def _export_segmented_upload_helper(data: typing.List[dict]):
        "Transform list of dicts to CSV format, uploads to S3 bucket"

        assert isinstance(data, list)

        if len(data) == 0:
            return

        buffer = io.StringIO()
        pd.DataFrame(data).to_csv(buffer, index=False)

        file_name = (
            f"{data_type}/{data_type}_api_{time.strftime(FORMAT_WRITETIME)}.csv"
        )
        bucket_destination.Object(key=file_name).put(Body=buffer.getvalue())

        print(f"Wrote file {file_name} with {len(data)} rows")

    if not hasattr(data_client, "query"):
        # If the data_client doesn't have a query method, try the "read" method

        data = data_client.read()[helper_to_camelCase(data_type)]
        global_num_calls_api += 1

        if isinstance(data, dict):
            # Assume only one record

            _export_segmented_upload_helper([data])
            int_total_results_cumulative = 1

        elif isinstance(data, list):
            # Assume mulitiple records

            _export_segmented_upload_helper(data)
            int_total_results_cumulative = len(data)

    else:
        # Data client has query method, proceed

        time_start: int = time.time()
        id_max: int = 0
        int_total_results_cumulative: int = 0
        list_buffer_results: typing.List[dict] = []
        while True:
            # ^ Yes, this is dangerous, but its necessary for allowing more information

            time.sleep(1)

            data_raw = data_client.query(
                format="json",
                sort_by="id",
                id_greater_than=id_max,
                updated_after=date_start.isoformat(),
                type=tagobject_type,
                **(
                    dict(created_after=date_start.isoformat())
                    if data_type in SET_DATA_TYPE_CREATED
                    else {}
                ),
            )

            global_num_calls_api += 1

            list_records = data_raw[helper_to_camelCase(data_type)]
            #                       ^ Key is camelCase
            _total_results = int(data_raw["total_results"])

            if _total_results == 0:
                break

            id_max = int(list_records[-1]["id"])
            if _total_results > int_total_results_cumulative:
                int_total_results_cumulative = _total_results

            list_buffer_results.extend(list_records)

            if len(list_buffer_results) >= INT_MAX_BUFFER:
                _export_segmented_upload_helper(list_buffer_results)
                list_buffer_results.clear()

            if _total_results < PARDOT_MAX_RESULT_COUNT:
                # Reached end of results, break
                break

            if time.time() > (time_start + TIMEOUT_SECONDS):
                raise Exception(
                    f"Timeout ({TIMEOUT_SECONDS} seconds) reached for data type {data_type !r}"
                )

        # Flush remaining results (if any)
        _export_segmented_upload_helper(list_buffer_results)

    print(
        f"Total results written for data type {data_type !r}: {int_total_results_cumulative}"
    )

    return int_total_results_cumulative

def pull_email_info(list_email_id,email_id,pardot_client):
    # Avoid reserved concurrency issue with sleep
    
    global global_num_calls_api

    time.sleep(1)

    headers = {
        "content-type": "application/json",
        "Authorization": f"Bearer {pardot_client.sftoken}",
        "Pardot-Business-Unit-Id": PARDOT_BUSINESS_UNIT_ID,
    }

    url = f"https://pi.pardot.com/api/email/version/4/do/read/id/{email_id}"
    response = requests.get(
            url,
            headers=headers)
    try:
        dom = ElementTree.fromstring(response.content)
        email_dom = dom.findall('email')[0]
        result = {
            "email":email_dom.text.replace('\n','').replace('  ',''),
            "email_id":email_dom.findall('id')[0].text,
            "list_email_id":list_email_id,
            "name":email_dom.findall('name')[0].text,
            "subject":email_dom.findall('subject')[0].text,
            "created_at":email_dom.findall('created_at')[0].text
            }
    except:
        print(response.content)

    global_num_calls_api += 1

    return result

def pull_missing_email_ids():
    """
    Pulls a list of all the list_email_ids that haven't been accounted for in the dev_data_vault.email table. V4 of 
    the Pardot API doesn't allow you to pull out a batch of emails, so a workaround is to grab one email_id for
    each list_email_id and extract the results (the results for every email_id across list_email_ids are the same)
    """
    ctx = get_client_snowflake()
    cs = ctx.cursor()

    # TODO, remove snowflake_query_2 when the tables are built
    snowflake_query = """select 
        a.list_email_id, min(a.email_id) 
        from dev_data_vault.marketing.visitor_activity a
        join (select distinct list_email_id from dev_data_vault.marketing.email) b
        on a.list_email_id = b.list_email_id
        where a.email_id is not null and a.list_email_id is not null 
        And b.list_email_id is null
        group by a.list_email_id
    """

    snowflake_query_2 = """select 
        a.list_email_id, min(a.email_id)
        from dev_data_vault.marketing.visitor_activity a
        where a.email_id is not null and a.list_email_id is not null 
        group by a.list_email_id
    """

    try:
        cs.execute(snowflake_query)
        missing_ids =  cs.fetchall()
    except:
        # If email table doesn't exist (first run I think?)
        cs.execute(snowflake_query_2)
        missing_ids =  cs.fetchall()   
    finally:
        cs.close()
    ctx.close()
    
    return missing_ids

def process_emails(data_type='Email'):

    emails = pull_missing_email_ids()
    print(f"Total expected Email results: {len(emails)}")
    p = get_client_pardot()

    data = [pull_email_info(email[0],email[1],p) for email in emails]

    def _export_segmented_upload_helper(data: typing.List[dict]):
        "Transform list of dicts to CSV format, uploads to S3 bucket"
        session_aws = get_session_boto()
        bucket_destination = session_aws.resource("s3").Bucket(AWS_NAME_BUCKET)

        assert isinstance(data, list)

        if len(data) == 0:
            return

        buffer = io.StringIO()
        pd.DataFrame(data).to_csv(buffer, index=False)

        file_name = (
            f"{data_type}/{data_type}_api_{time.strftime(FORMAT_WRITETIME)}.csv"
        )
        bucket_destination.Object(key=file_name).put(Body=buffer.getvalue())

        print(f"Wrote file {file_name} with {len(data)} rows")

    _export_segmented_upload_helper(data)

    return

  

if __name__ == "__main__":

    list_data_type_bulk = [
        "ListMembership",
        "Prospect",
        "ProspectAccount",
        "Visitor",
        "VisitorActivity",
    ]

    list_data_type_segmented = [
        "Campaign",
        "Form",
        "Tag",
        "Opportunity",
        "EmailClick",
        "List",
        "Account",
    ]

    # TagObject has a massive amount of data, so only query for these three
    # You can't specify "in" with the API, only a single type at a time
    tag_object_types = ["Email","Campaign","List"]

    try:

        for data_type in list_data_type_bulk:
            print(f"Starting bulk export for {data_type !r}")
            export_bulk(data_type)

        for data_type in list_data_type_segmented:
            print(f"Starting segmented export for {data_type !r}")
            export_segmented(data_type)

        for tag_type in tag_object_types:
            print(f"Starting segment export for TagObject type {tag_type !r}")
            export_tagobject_segmented(tagobject_type=tag_type)

        print("Starting segmented export for 'Email'")
        process_emails()

    except PardotAPIError as err:
        if err.err_code in [
            184,
            122,
        ]:
            # "Error #184: access_token is invalid, unknown, or malformed",
            # "Error #122: Daily API rate limit met",

            session = get_session_boto()
            session.get_credentials()

            session.client(
                "sns",
                region_name=AWS_DEFAULT_REGION,
            ).publish(
                TargetArn=AWS_SNS_TOPIC_ARN_EMAIL_NOTIFICATION,
                Message=f"PARDOT ERROR: {err.message}",
                MessageStructure="string",
            )
        raise
    else:
        print('Process Finished')
        # session = get_session_boto()
        # session.get_credentials()

        # session.client(
        #     "sns",
        #     region_name=AWS_DEFAULT_REGION,
        # ).publish(
        #     TargetArn=AWS_SNS_TOPIC_ARN_EMAIL_NOTIFICATION,
        #     Message=f"PARDOT SUCCESS: Number of API calls: {global_num_calls_api :,}",
        #     MessageStructure="string",
        # )
