import boto3

def create_s3_client():
    AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
    AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    return s3_client

def download_s3_file(s3_client, bucket_name, key, filename):
    """
    Download a file from an S3 bucket.
    :param s3_client: Authenticated S3 Client
    :param bucket_name: S3 bucket name
    :param key: S3 key for the file to download
    :param filename: local path to save the file
    """
    s3_client.download_file(
        Bucket=bucket_name,
        Key=key,
        Filename=filename
    )
    print(f"Downloaded {key} to {filename}")