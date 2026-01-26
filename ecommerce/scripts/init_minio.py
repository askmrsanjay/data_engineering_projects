import boto3
from botocore.client import Config

def init_minio():
    """Initialize the required buckets in MinIO."""
    s3 = boto3.resource('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='admin',
                    aws_secret_access_key='password123',
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

    buckets = ['ecommerce-bucket']
    
    for bucket_name in buckets:
        bucket = s3.Bucket(bucket_name)
        if bucket.creation_date:
            print(f"Bucket '{bucket_name}' already exists.")
        else:
            s3.create_bucket(Bucket=bucket_name)
            print(f"Created bucket: '{bucket_name}'")

if __name__ == "__main__":
    init_minio()
