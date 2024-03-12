import json
import requests
from vars import *
from dynamodb import normal_dict_to_dynamodb_item, App as DynamoApp
from s3 import upload_file
import datetime
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(category=InsecureRequestWarning)
from decimal import Decimal
from os.path import isfile, join
from os import listdir


def flatten(d):
    res = {}
    for item in d:
        if isinstance(d[item], dict):
            for key, val in d[item].items():
                res.update({key: val})
        else:
            res.update({item: d[item]})
    return res


def get_request(curr, date=None):
    # Check if date exists. If not use 'latest' instead.
    date = date if date else 'latest'

    # Construct URL
    url = f"{base_url}{api_endpoint}@{date}/v1/currencies/{curr}.json"

    # Send request
    resp = requests.request("GET", url, verify=False).json()

    return resp


def format_response(resp):
    resp[base_curr] = {val: resp[base_curr][val] for val in curr_exchange}
    resp = flatten(resp)

    return resp


def get_response():
    resp = get_request(curr=base_curr)
    return format_response(resp)


def get_batch_response(no_days):
    date_list = [datetime.datetime.today() - datetime.timedelta(days=x) for x in range(no_days)]
    date_list = [item.strftime('%Y-%m-%d') for item in date_list]
    response = []
    for date in date_list:
        resp = get_request(curr=base_curr, date=date)
        response.append(format_response(resp))
    return response


if __name__ == '__main__':
    # Get API data
    print('Calling API.')
    response = get_response()

    # Serialize dict to dynamodb type.
    print('Serialize response for dynamodb.')
    single_response = normal_dict_to_dynamodb_item(response)

    # Generate Client
    print('Establish dynamodb connection client.')
    client = DynamoApp(region_name='eu-north-1')
    client.generate_client()

    # Check if table exists and create one if not.
    print('Try to create table if it does not exist.')
    client.create_table(
        table_name=table_name,
        table_attribute_def=[{
                'AttributeName': 'date',
                'AttributeType': 'S',
            }],
        table_key_schema=[{
                'AttributeName': 'date',
                'KeyType': 'HASH',
            }],
        table_throughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10,
        })

    # Add all items to DynamoDB
    client.put_items(table_name, items=single_response)

    print('Read table entries.')
    table_items = client.read_table(table_name)
    table_items = [{k: (float(v) if isinstance(v, Decimal) else v) for k, v in item.items()} for item in table_items]
    table_items = [{item['date']: item} for item in table_items]

    with open(file='data.json', mode='w') as f:
        json.dump(table_items, f)

    file_name = 'data.json'
    upload_file(file_name=file_name, bucket=s3_bucket)
