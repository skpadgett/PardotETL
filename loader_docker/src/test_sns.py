import os
import json

import boto3

AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = "us-east-1"
AWS_ACCESS_KEY_ID = "ASIA3FXKNT2YFYRNSQVA"
AWS_SECRET_ACCESS_KEY = "eQx9yJD6SuFzqNUrW4pd9s8w8Eae1GvRpgJk1ENI"
AWS_SESSION_TOKEN = "IQoJb3JpZ2luX2VjEHAaCXVzLWVhc3QtMSJHMEUCIAKfwH6FxUwMFyEFG7qq5okgldB2lD/7Bvw8Prd9kU2SAiEAs3INkY4mSlS88kf9loZ4wvec+/sO/7E5RSAwFVHojKUqnAMIeBAAGgw3NjgyMTcwMzAzMjAiDLFakHsOA6UmlikEDCr5ArjYiDPd7YBv4UoHVWwLvEF8NRbNFlSCJWrXstWHaZajG41cnM4UzacHThMt47HdizxzGRTAs49SHr6C4FBHJmRlgU3mpd8SkKHeuazO3hdwlxxQXxnBZAsmmwUIB9YGEsAoSH+87e3G147GT7MNvHu6fW0KBZsoac4XkM85dSLrJKVHavnV8EhjlDRWlmLhbx6Ntoz0S3P/gAggbZnxy27/lfzBL9jtCZqnOUEHbXQdKPCThg1BzRnlbHIY73m052x9WvH4kHX3WofKU9EsMPXCcx4JlXCM4s+yyu0mfNNul+v9htdVqeLwtfo19T5BX463geb6BOZ1EpN2WbYL3Yfe7od57A4gabGlHkeAHbb2Y4I9iLvSNDad29LM1FheZ+fmtTbaItH1IKLpGXHxmM9X4avt+pVJJHduBe8cQkpXo73J7o7FEhtp9dosKUK8PBDUB2vPrlYgei+U2XM9yoL1HZZiNnLK5l/KoGlWuzctfxrr0jCO5SI0MOjlhYgGOqYBY/U2YrxZjpIeQuWzjOKtnphmRNzFcBkPSdCSiwjnCWP1rMAn8b87smta7HAa2y2sdisiaaxiP4BE02LH2pBr9QlZS7kto3RBTqYtojgrcBRrr4qayNtNKkCJ8Z9V3CzOJtYmsUWzBJ4uHl8XaKlvSxhOwNGhknbV3KRc9zeIde794T1ZMv0wOHH+AmORzYHreL/FYPj6ixOZlgSxhql7IW9lEhs9Eg=="


message = {"hello": "there"}
# session = boto3.Session(
#     aws_access_key_id=AWS_ACCESS_KEY_ID,
#     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#     region_name=AWS_REGION
#     # aws_session_token=AWS_SESSION_TOKEN,
# )
client = boto3.client(
    "sns",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION,
)

response = client.publish(
    TargetArn="arn:aws:sns:us-east-1:768217030320:test-topic-email-notification",
    Message="Hello there!",
    # Message=json.dumps({"default": json.dumps(message)}),
    MessageStructure="string",
)
