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

# Gather environmental variables
PARDOT_EMAIL = os.environ["pardotEmail"]
PARDOT_USER_KEY = os.environ["pardotUserKey"]
PARDOT_SF_CONSUMER_KEY = os.environ["pardotSfConsumerKey"]
PARDOT_SF_CONSUMER_SECRET = os.environ["pardotSfConsumerSecret"]
PARDOT_SF_REFRESH_TOKEN = os.environ["pardotSfRefreshToken"]
PARDOT_BUSINESS_UNIT_ID = os.environ["pardotSfBusinessUnitID"]
PARDOT_API_VERSION = os.environ["pardotVersion"]
PARDOT_MAX_RESULT_COUNT = 200
# AWS_NAME_BUCKET = os.environ.get("s3FileStore", "test-pardot-etl")
AWS_NAME_BUCKET = os.environ.get(
    "AWS_NAME_BUCKET", "pardot-us-prod-20210729173046836600000001"
)
# "de-sandbox-us-east-2"
# AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
# AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
# AWS_SESSION_TOKEN = os.environ["AWS_SESSION_TOKEN"]
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
    "EmailStat": "emailstats",
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
        # aws_access_key_id=AWS_ACCESS_KEY_ID,
        # aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        # aws_session_token=AWS_SESSION_TOKEN,
    )


def get_date_start_snowflake(data_type: str = None) -> dt.date:
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

    cs = ctx.cursor()
    try:
        cs.execute(
            f"""SELECT max({name_field_table})
            FROM {name_table_snowflake}
            WHERE {name_field_table} IS NOT null"""
        )
        one_row = cs.fetchone()
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

    # # Placeholder
    # return dt.date.today() - dt.timedelta(days=1)


def test_get_date_start_snowflake():
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
        # "TagObject",
        "Opportunity",
        # "EmailStat",
        "EmailClick",
        "List",
        "Account",
    ]

    for data_type in list_data_type_bulk + list_data_type_segmented:
        print(f"Running date grab for data type {data_type !r}")

        result = get_date_start_snowflake(data_type)

        print(f"Result for date grab for {data_type !r} is {result !r}")

        assert isinstance(
            result, dt.date
        ), f"Returned type of data type {data_type !r} is not a date"


def test_connection_pardot():
    p = get_client_pardot()

    print(
        "Running test query: number of prosepects created after day before yesterday..."
    )

    prospects = p.prospects.query(
        created_after=(dt.datetime.now() - dt.timedelta(days=2)).isoformat()
    )
    total = prospects["total_results"]  # total number of matching records

    print(f"Total results: {total}")
    # print(prospects)
    print(next(iter(prospects["prospect"])))

    # p.visitoractivities.query(created_after=dt.datetime.now().isoformat())
    # print(p)


def test_connection_aws():
    s = get_session_boto()
    print(s.client("s3").list_objects(Bucket=AWS_NAME_BUCKET))
    # print(dir(s))


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


def test_connection_snowflake():
    print("Starting snowflake connection")
    ctx = get_client_snowflake()

    print("Starting snowflake test query")
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()


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
    SECONDS_SLEEP = 5
    while time.time() < (time_start + TIMEOUT_SECONDS):
        i += 1

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
        global_num_calls_api += 1

        state_export: str = response_status.json()["export"]["state"]
        if state_export in ["Waiting", "Processing"]:
            time.sleep(SECONDS_SLEEP)
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
            # f"test/{data_type}/{data_type}_api_{time.strftime(FORMAT_WRITETIME)}.csv"
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
        # Data client has query method, proceeed

        time_start: int = time.time()
        id_max: int = 0
        int_total_results_cumulative: int = 0
        list_buffer_results: typing.List[dict] = []
        while True:
            # ^ Yes, this is dangerous, but its necessary for allowing more information

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
            int_total_results_cumulative += _total_results

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


if __name__ == "__main__":

    # test_connection_snowflake()
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
        # "TagObject",
        "Opportunity",
        # "EmailStat",
        "EmailClick",
        "List",
        "Account",
    ]

    try:
        test_connection_pardot()
        test_connection_aws()
        # export_bulk("VisitorActivity")
        # export_segmented("Campaign")
        # export_segmented("Tag")
        # export_segmented("ProspectAccounts")
        # test_get_date_start_snowflake()

        for data_type in list_data_type_bulk:
            print(f"Starting bulk export for {data_type !r}")
            export_bulk(data_type)

        for data_type in list_data_type_segmented:
            print(f"Starting segmented export for {data_type !r}")
            export_segmented(data_type)

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
                # aws_access_key_id=AWS_ACCESS_KEY_ID,
                # aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                # aws_session_token=AWS_SESSION_TOKEN,
            ).publish(
                TargetArn=AWS_SNS_TOPIC_ARN_EMAIL_NOTIFICATION,
                Message=f"PARDOT ERROR: {err.message}",
                # Message=json.dumps({"default": json.dumps(message)}),
                MessageStructure="string",
            )
        raise
    else:
        session.client(
            "sns",
            region_name=AWS_DEFAULT_REGION,
            # aws_access_key_id=AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            # aws_session_token=AWS_SESSION_TOKEN,
        ).publish(
            TargetArn=AWS_SNS_TOPIC_ARN_EMAIL_NOTIFICATION,
            Message=f"PARDOT SUCCESS: Number of API calls: {global_num_calls_api :,}",
            # Message=json.dumps({"default": json.dumps(message)}),
            MessageStructure="string",
        )
