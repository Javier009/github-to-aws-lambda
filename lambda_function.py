import boto3
import pandas as pd
print("Boto3 version:", boto3.__version__)

client = boto3.client('s3')
response = client.list_objects(Bucket='test-aws-cli-created-bucket')

def lambda_handler(event, context):
    for obj in response.get('Contents', []):
        file_size = obj['Size']
        
        # REad if file size is greater less than 100MB
        if file_size < 100 * 1024 * 1024:  # 100MB in bytes
            s3_object = client.get_object(Bucket='test-aws-cli-created-bucket', Key=obj['Key'])
            df = pd.read_csv(s3_object['Body'])
            print(f"Contents of {obj['Key']}:")
            print(df.head())  # Print first few rows of the dataframe
        else:
            print(f"Skipping {obj['Key']} due to size > 100MB")
        
    return {'statusCode': 200, 'body': 'S3 files processed successfully'}
    