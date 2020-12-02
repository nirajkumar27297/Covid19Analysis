import boto3
import botocore
import os

def save_to_s3(bucket_name, file_name):

    # try:

    s3 = boto3.resource("s3",aws_access_key_id=os.environ["AWS_ACCESS_KEY"],aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    if not s3.Bucket(bucket_name) in s3.buckets.all():
        s3.create_bucket(acl='public-read',
            Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
        )
    s3_client = boto3.client("s3",aws_access_key_id=os.environ["AWS_ACCESS_KEY"],aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"])
    s3_client.delete_object(Bucket=bucket_name,Key=file_name)
    s3.Bucket(bucket_name).upload_file(file_name,file_name)



    # except botocore.exceptions.ClientError as ex:
    #     print("Invalid Bucket Name or Bucket Already Exists\n", ex)
    # except FileNotFoundError as ex:
    #     print("Path Specified Is Wrong", ex.with_traceback())
    # except Exception as ex:
    #     print(ex.with_traceback())
    #     raise Exception("Unexpected Error Occured")
    # else:
    #     print("File Uploaded Successfully")
save_to_s3("savedcoviddata","output.csv")