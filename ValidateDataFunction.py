import boto3
import csv
import os

# Initialize the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Extract bucket name and file key from the Step Functions input
    bucket_name = event['detail']['bucket']['name']
    file_key = event['detail']['object']['key']

    # Download the file from S3
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
    except Exception as e:
        return {
            'status': 'Fail',
            'error_message': f"Error getting object {file_key} from bucket {bucket_name}. Make sure they exist and your bucket is in the same region as this function.",
            'error_info': str(e)
        }
    
    # Read the content of the file
    content = response['Body'].read().decode('utf-8')
    lines = content.splitlines()
    
    # Check if the file is a CSV by attempting to read the first line as headers
    try:
        headers = next(csv.reader(lines))
    except Exception as e:
        return {
            'status': 'Fail',
            'error_message': f"Failed to read the headers from the file {file_key}.",
            'error_info': str(e)
        }
    
    # Define the required headers
    required_headers = ["Date", "Product_ID", "Quantity", "Unit_Price", "Total_Sales", "Location", "Discount_Rate"]

    # Validate the headers
    if headers == required_headers:
        return {'status': 'Success'}
    else:
        return {'status': 'Fail', 'error_message': 'The file does not have the required headers.'}

