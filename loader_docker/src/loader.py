import io
import time
import json
import datetime as dt
import typing
import requests
from operator import itemgetter
from pprint import pprint
from os import environ

import pandas as pd
import boto3
import snowflake.connector
from pypardot.client import PardotAPI
from smart_open import smart_open


# Gather environmental variables
PARDOT_EMAIL = environ.get("pardotEmail")
PARDOT_USER_KEY = environ.get("pardotUserKey")
PARDOT_SF_CONSUMER_KEY = environ.get("pardotSfConsumerKey")
PARDOT_SF_CONSUMER_SECRET = environ.get("pardotSfConsumerSecret")
PARDOT_SF_REFRESH_TOKEN = environ.get("pardotSfRefreshToken")
PARDOT_BUSINESS_UNIT_ID = environ.get("pardotSfBusinessUnitID")
PARDOT_API_VERSION = environ.get("pardotVersion")
PARDOT_MAX_RESULT_COUNT = 200
AWS_NAME_BUCKET = environ.get("s3FileStore", "test-pardot-etl")
AWS_ACCESS_KEY_ID = environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = environ.get("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = environ.get("AWS_SESSION_TOKEN")
SNOWFLAKE_ACCOUNT_IDENTIFIER = "UK29315"
SNOWFLAKE_USER = "clroberts@discoveryed.com"
SNOWFLAKE_PASS = r'$k{Xc:9l._n,3$e&V "G'

TIMEOUT_SECONDS = 60 * 60 * 6  # <-- 6 Hour timeout
PARDOT_URL_API = f"https://pi.pardot.com/api/export/version/{PARDOT_API_VERSION}"
FORMAT_WRITETIME = "%Y%m%d-%H%M%S"
INT_MAX_BUFFER = 1000
# ^ Limit number of records of output file for segmented export to this number

DICT_CONVERSION_PURAL = {
    "VisitorActivity": "visitoractivities",
    "Campaign": "campaigns",
    "List": "lists",
    "Prospect": "prospects",
    "TagObject": "tagobjects",
    "Tag": "tags",
    "Opportunity": "opportunities",
    "ProspectAccounts": "prospectaccounts",
    "Form": "forms",
    "ListMemberships": "listmemberships",
    "Visitor": "visitors",
}


def get_client_pardot() -> PardotAPI:
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

    assert (
        p.sftoken != "dummy"
    ), "Pardot authentication still unsuccessful after querying"

    return p


def get_session_boto() -> boto3.Session:
    return boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )


def get_date_start_snowflake(data_type: str = None) -> dt.date:
    "Return the latest updated_at date in Snowflake for the specified data type"

    # Placeholder
    return dt.date.today() - dt.timedelta(days=1)


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
    print(dir(s))


def test_connection_snowflake():
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        # password=SNOWFLAKE_PASS,
        account=SNOWFLAKE_ACCOUNT_IDENTIFIER,
        authenticator="externalbrowser",
        warehouse="GENERAL_COMPUTE_WH",
        database="DEV_DATA_VAULT",
        schema="STAGE",
    )
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
                        ).isoformat(),  # "2021-01-01 00:00:01",
                        # "updated_before": (
                        #     dt.date.today() - dt.timedelta(days=0)
                        # ).isoformat(),  # "2021-01-02 00:00:01",
                    },
                },
            }
        ),
    )

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

        file_name = (
            f"test/{data_type}/{data_type}_bulk_{time.strftime(FORMAT_WRITETIME)}.csv"
        )
        bucket_destination.Object(key=file_name).put(Body=response_data.text)

        _num_rows: int = response_data.text.count("\n")
        print(f"Sent {file_name} to {AWS_NAME_BUCKET} with {_num_rows} records")


def export_segmented(data_type: str) -> int:
    "API export through regular, segmented API calls"

    assert (
        data_type in DICT_CONVERSION_PURAL
    ), f"{data_type} not defined in conversion map (you may need to add this in the code)"

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
        pd.DataFrame(data).to_csv(buffer)

        file_name = (
            f"test/{data_type}/{data_type}_api_{time.strftime(FORMAT_WRITETIME)}.csv"
        )
        bucket_destination.Object(key=file_name).put(Body=buffer)

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
        )

        list_records = data_raw[data_type.lower()]
        _total_results = int(data_raw["total_results"])

        id_max = int(list_records[-1]["id"])
        int_total_results_cumulative += _total_results

        list_buffer_results.extend(list_records)

        if len(list_buffer_results) > INT_MAX_BUFFER:
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

    return int_total_results_cumulative


if __name__ == "__main__":
    # test_connection_snowflake()
    # test_connection_pardot()
    # test_connection_aws()
    # export_bulk("VisitorActivity")
    export_segmented("Campaign")
