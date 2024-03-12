import boto3
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
from botocore.exceptions import ClientError
from vars import *
from time import sleep
import json
from decimal import Decimal

def normal_dict_to_dynamodb_item(normal_dict):
    """
    Take a DynamoDB formatted dictionary and change it to normal JSON format
    :param normal_dict: DynamoDB formatted dictionary
    :return: JSON formatted dictionary
    """

    serializer = TypeSerializer()
    if isinstance(normal_dict, dict):
        serialized_resp = [{k: (serializer.serialize(v) if not isinstance(v, float) else serializer.serialize(Decimal(str(v)))) for k, v in normal_dict.items()}]
    elif isinstance(normal_dict, list):
        serialized_resp = []
        for item in normal_dict:
            serialized_item = {
                k: (serializer.serialize(v) if not isinstance(v, float) else serializer.serialize(Decimal(str(v)))) for
                k, v in item.items()}
            serialized_resp.append(serialized_item)

    return serialized_resp


def dynamodb_item_to_normal_dict(dynamodb_item):
    """
    Take a DynamoDB formatted dictionary and change it to normal JSON format
    :param dynamodb_item: DynamoDB formatted dictionary
    :return: JSON formatted dictionary
    """

    deserializer = TypeDeserializer()
    deserialized_item = {
        k: deserializer.deserialize(v)
        for k, v in dynamodb_item.items()
    }

    return deserialized_item


class App():
    def __init__(self, region_name):
        self.client = None
        self.service = 'dynamodb'
        self.region = region_name

    def generate_client(self):
        self.client = boto3.client(service_name=self.service, region_name=self.region,
                      aws_access_key_id='AKIAYS2NV7OMBKFDR5RN',
                      aws_secret_access_key='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')
        print('Client generated.')
        return None

    def list_tables(self):
        return self.client.list_tables(Limit=10)

    def create_table(self, table_name, table_attribute_def, table_key_schema, table_throughput):
        try:
            self.client.create_table(
                AttributeDefinitions=table_attribute_def,
                KeySchema=table_key_schema,
                ProvisionedThroughput=table_throughput,
                TableName=table_name
            )

            while 'ACTIVE' != self.client.describe_table(TableName=table_name)['Table']['TableStatus']:
                print('Table is not yet active.')
                sleep(5)

            print('Table is active.')

        except ClientError as e:
            print(f'{e.response["Error"]["Code"]}. {e.response["Error"]["Message"]}')
        return None

    def put_items(self, table_name, items):
        for item in items:
            self.client.put_item(
                TableName=table_name,
                Item=item
            )
            print('Item put.')
        return None


if __name__ == '__main__':

    client = boto3.client('dynamodb',
                          aws_access_key_id='AKIAYS2NV7OMBKFDR5RN',
                          aws_secret_access_key='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',
                          region_name='eu-north-1')

    # Check if table already exits.
    table_list = client.list_tables(Limit=10)

    if table_name not in table_list['TableNames']:
        print("Creating table.")

        # Create a query request
        response = client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'date',
                    'AttributeType': 'S',
                },
            ],
            KeySchema=[
                {
                    'AttributeName': 'date',
                    'KeyType': 'HASH',
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10,
            },
            TableName='Currency',
        )
        print("Creating table. - DONE")

    # Wait for table to be created if it doesn't exist
    while table_name not in table_list['TableNames']:
        # Check if table was created.
        table_list = client.list_tables(Limit=10)
        sleep(5)


    print("Table exists. Adding Item.")
    response = client.put_item(
        Item={
            'Date': {
                'S': '2024-03-07',
            },
            'eur': {
                'N': '0.25',
            },
            'usd': {
                'N': '0.25',
            },
            'mdl': {
                'N': '4.25',
            },
        },
        TableName='Currency',
    )

    print("Adding Item. - DONE")

    print(response)

