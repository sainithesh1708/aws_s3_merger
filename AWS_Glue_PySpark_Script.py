import boto3
import json
import time
import pandas as pd
import sys  # <-- Import sys module
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from io import StringIO


print("Initialize the Glue context and Spark context")
# Initialize the Glue context and Spark context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Initialize the job with the correct name
args = getResolvedOptions(sys.argv, ['JOB_NAME'])  # <-- Ensure correct argument
job.init(args['JOB_NAME'], args)

# DynamoDB client
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

# Parameters for DynamoDB table and S3 buckets
DYNAMODB_TABLE = 's3-file-upload-metadata'  # Replace with your DynamoDB table name
MERGED_BUCKET = 's3-two-file-merger-v1'  # Replace with your target S3 bucket for merged files

# Function to get the next two unprocessed files from DynamoDB
def get_unprocessed_files():
    try:
        response = dynamodb.scan(
            TableName=DYNAMODB_TABLE,
            FilterExpression="#processed = :val",
            ExpressionAttributeNames={
                "#processed": "processed"  # Map the reserved keyword to an alias
            },
            ExpressionAttributeValues={
                ":val": {"BOOL": False}  # Filter for unprocessed files
            }
        )
        print(f"DynamoDB scan response: {response}")  # Debugging
        files = response.get('Items', [])
        if len(files) < 2:
            return None
        sorted_files = sorted(files, key=lambda x: x['timestamp']['S'])
        return sorted_files[:2]
    except Exception as e:
        print(f"Error scanning DynamoDB: {e}")
        raise



# Function to download file from S3
def download_file(bucket_name, file_key):
    # Get the S3 object
    s3_object = s3.get_object(Bucket=bucket_name, Key=file_key)
    file_content = s3_object['Body'].read().decode('utf-8')  # Assuming it's a CSV file
    return pd.read_csv(pd.compat.StringIO(file_content))

# Function to upload merged file to S3
def upload_file_to_s3(bucket_name, file_key, content):
    # Upload the merged content back to S3
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=content)

def merge_files():
    try:
        # Step 1: Retrieve unprocessed files from DynamoDB
        files_to_merge = get_unprocessed_files()  # This function gets unprocessed files from DynamoDB

        if len(files_to_merge) < 2:
            print("Not enough files to merge. Exiting.")
            return

        # Step 2: Download the files from S3
        file_1 = files_to_merge[0]
        file_2 = files_to_merge[1]

        # Retrieve files from S3
        file_1_obj = s3.get_object(Bucket=file_1["bucket"]["S"], Key=file_1["filename"]["S"])
        file_2_obj = s3.get_object(Bucket=file_2["bucket"]["S"], Key=file_2["filename"]["S"])

        # Step 3: Read CSVs into Pandas DataFrames
        file_1_df = pd.read_csv(StringIO(file_1_obj["Body"].read().decode('utf-8')))
        file_2_df = pd.read_csv(StringIO(file_2_obj["Body"].read().decode('utf-8')))

        # Step 4: Find common columns (intersection of column names)
        common_columns = list(set(file_1_df.columns).intersection(set(file_2_df.columns)))

        if not common_columns:
            print("No common columns found. Exiting.")
            return

        print(f"Common columns: {common_columns}")

        # Step 5: Perform the merge on common columns (inner join to keep only matching rows)
        merged_df = pd.merge(file_1_df, file_2_df, on=common_columns, how="inner")

        # Step 6: Save the merged data to a new CSV
        output_buffer = StringIO()
        merged_df.to_csv(output_buffer, index=False)
        output_buffer.seek(0)

        # Step 7: Upload the merged file back to S3
        merged_file_key = f"merged_files/{file_1['filename']['S'].split('.')[0]}_{file_2['filename']['S'].split('.')[0]}_merged.csv"
        s3.put_object(Bucket=MERGED_BUCKET, Key=merged_file_key, Body=output_buffer.getvalue())

        print(f"Merged file uploaded to: {merged_file_key}")

        # Step 8: Mark both files as processed
        mark_as_processed(file_1)
        mark_as_processed(file_2)

    except Exception as e:
        print(f"Error in merge_files: {e}")
        raise


def mark_as_processed(file_entry):
    dynamodb = boto3.client("dynamodb")
    try:
        dynamodb.update_item(
            TableName=DYNAMODB_TABLE,
            Key={
                "filename": {"S": file_entry["filename"]["S"]}
            },
            UpdateExpression="SET #processed = :val",
            ExpressionAttributeNames={
                "#processed": "processed"
            },
            ExpressionAttributeValues={
                ":val": {"BOOL": True}
            }
        )
        print(f"Marked file {file_entry['filename']['S']} as processed.")
    except Exception as e:
        print(f"Error marking file as processed: {e}")
        raise

# Run the merge operation
merge_files()

job.commit()
