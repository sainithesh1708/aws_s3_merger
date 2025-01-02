import json
import boto3
from datetime import datetime

# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')

DYNAMODB_TABLE = 's3-file-upload-metadata'  # Replace with your DynamoDB table name

def lambda_handler(event, context):
    try:
        
        for sqs_record in event['Records']:

            message_body = json.loads(sqs_record['body'])
            
            s3_records = message_body.get('Records', [])
            
            for s3_record in s3_records:
                bucket_name = s3_record['s3']['bucket']['name']
                object_key = s3_record['s3']['object']['key']
                

                item = {
                    'filename': {'S': object_key},
                    'bucket': {'S': bucket_name},
                    'timestamp': {'S': datetime.utcnow().isoformat()},
                    'processed': {'BOOL': False}
                }
                
                dynamodb.put_item(
                    TableName=DYNAMODB_TABLE,
                    Item=item
                )
                
                print(f"Inserted item into DynamoDB: {item}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Items successfully added to DynamoDB!')
        }
    
    except Exception as e:
        print("Error:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error adding items to DynamoDB: {str(e)}')
        }
