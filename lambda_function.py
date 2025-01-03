import json
import requests
import pandas as pd
import os
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    url = "https://Real-Time-Events-Search.proxy-production.allthingsdev.co/v1/search-events?query=Concerts+in+India&date=any&is_virtual=false&start=0"

    headers = {
        'x-apihub-key': 'wU5NX17gF8SekT7hadF0SjIMPh-EMr1TgVfLBkxtkZdmukax9M',
        'x-apihub-host': 'Real-Time-Events-Search.allthingsdev.co',
        'x-apihub-endpoint': '0ac072ed-7872-4f4c-9304-7a8252d633b3'
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Error {response.status_code}: {response.text}")

    client = boto3.client('s3')
    filename = 'real_time_events_raw_data' + str(datetime.now()) + '.json'
    client.put_object(
        Body=json.dumps(data),
        Bucket='real-time-events-search-amar',
        Key='raw_data/to_process/' + filename
    )
