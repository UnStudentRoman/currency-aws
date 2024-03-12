import logging
import boto3
from botocore.exceptions import ClientError
import sys
import threading
from os import listdir, environ
from os.path import isfile, join, getsize, basename



class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',
                             aws_access_key_id=environ.get('AWS_ACCESS_KEY'),
                             aws_secret_access_key=environ.get('AWS_SECRET_ACCESS_KEY'),
                             region_name='eu-north-1')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name,
                                         Callback=ProgressPercentage(file_name))
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == '__main__':
    bucket = 'cmarius97-s3-test'
    # file_name = 'test.json'

    onlyfiles = [f for f in listdir('website') if isfile(join('website', f))]

    for file in onlyfiles:
        upload_file(file_name=f'website/{file}', bucket=bucket, object_name=file)