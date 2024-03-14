import boto3
from botocore.exceptions import ClientError
from os import environ


def get_secret():

    secret_name = "secret_token"
    region_name = "eu-north-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        aws_access_key_id = environ.get('AWS_ACCESS_KEY'),
        aws_secret_access_key = environ.get('AWS_SECRET_ACCESS_KEY')
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']

    print(secret)


if __name__ == '__main__':
    get_secret()