# PardotETL

This ETL is used to extract the following data from SalesForce and upload to an S3 bucket:

* "ListMembership"
* "Prospect"
* "ProspectAccount"
* "Visitor"
* "VisitorActivity"
* "Campaign"
* "Form"
* "Tag"
* "TagObject"
* "Opportunity"
* "EmailClick"
* "List"
* "Account"
* "Email"

For more information on these objects, see [here](https://developer.salesforce.com/docs/marketing/pardot/guide/overview.html)

The ETL interacts with the Pardot API via the [PyPardotSF](https://pypi.org/project/PyPardotSF/) python library.

The data objects above are broken up into 2 categories- bulk and segmented. 

Pardot APIs allow for bulk exports for:
* "ListMembership"
* "Prospect"
* "ProspectAccount"
* "Visitor"
* "VisitorActivity"

The process of a bulk exports is as follows:
* Make a HTTP request for a bulk export of a data object (CSV) based on a specified time frame (this timeframe is determined by querying for the latest timestamp in the associated SnowFlake table and subtracting 1 day from it). If a date doesn't exist, it will use a default date of 01-01-1900 to ensure a full historical load is done.
* Check the status of the export every minute using HTTP requests
* Once the status turns to complete, loop through each of the files (they generate multiple files for a bulk export) and upload to the S3 bucket

Pardot APIs allow for segmented exports for:
* "Campaign"
* "Form"
* "Tag"
* "TagObject"
* "Opportunity"
* "EmailClick"
* "List"
* "Account"
* "Email"

The process of segmented exports is as follows:
* Make a HTTP request for a 200 record export of a data object (CSV) based on a specified time frame (this timeframe is determined by querying for the latest timestamp in the associated SnowFlake table and subtracting 1 day from it). If a date doesn't exist, it will use a default date of 01-01-1900 to ensure a full historical load is done.
* There will be multiple paginated requests made until all the data has been extracted.
* As these requests are being made, upload a file each time the total count reaches 1000 records (except the scenario where there are less than 1000 total records left... aka the end of the process)


*Note, the process for the email object is different because you cannot query by a timeframe for all relevant emails in the v4 of Pardot's API. For this, the code searches visitor_activity for all list_email_id's and the minimum email_id associated with it. From here an HTTP request for every single email_id is made. Once all the records have been collected, a file is uploaded to S3. The reason that the minimum email_id is collected is because the email metadata for any one list_email_id is identical across all email_id's (aka list_email_id has a many-to-1 relationship with email_id)*

# Deployment

To deploy this to ECS in PROD:
1. Open the terminal
2. Go inside of the loader_docker folder
3. Grab the production AWS credentials from the AWS account page (BulkImport Prod -> DE_Developer -> Command    line or programmatic access). Paste these into the terminal
4. Run "sh ProdDeploy.sh"

The process is scheduled via cron to run daily, but you can find instructions to run manually [here](https://github.com/discoveryedu/tf-etl/tree/develop/pardot)

To deploy this to ECS in DEV:
1. Open the terminal
2. Go inside of the loader_docker folder
3. Grab the dev AWS credentials from the AWS account page (DBS PreProd -> DE_Developer -> Command line or programmatic access). Paste these into the terminal.
4. Run "sh DevDeploy.sh"

The process is scheduled via cron to run daily, but you can find instructions to run manually [here](https://github.com/discoveryedu/tf-etl/tree/develop/pardot)
