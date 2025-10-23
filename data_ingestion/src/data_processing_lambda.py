import os
import json
import logging
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# AWS S3 client
s3_client = boto3.client('s3')

# Environment variables
PROCESSED_S3_BUCKET = os.environ.get('PROCESSED_DATA_S3_BUCKET', 'cb-global-processed-financial-data')
PROCESSED_S3_PREFIX = os.environ.get('S3_PROCESSED_DATA_PREFIX', 'processed_data')

def load_raw_data_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    """Loads a JSON file from S3."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        logger.info(f"Successfully loaded raw data from s3://{bucket}/{key}")
        return data
    except ClientError as e:
        logger.error(f"Error loading data from s3://{bucket}/{key}: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error for s3://{bucket}/{key}: {e}")
        raise

def transform_financial_data(raw_data: Dict[str, Any], data_type: str) -> Dict[str, Any]:
    """Applies transformations to raw financial data based on its type.
    This is a placeholder for actual business logic and data cleaning/harmonization.
    """
    logger.info(f"Transforming data of type: {data_type}")
    processed_data = []
    if data_type == 'company_financials':
        for record in raw_data:
            # Example transformation: standardize keys, calculate ratios, filter irrelevant fields
            transformed_record = {
                'symbol': record.get('symbol'),
                'date': record.get('date'),
                'revenue': record.get('revenue'),
                'net_income': record.get('netIncome'),
                'currency': record.get('currency'),
                # Add more transformations as needed
            }
            if all(transformed_record.values()): # Basic validation
                processed_data.append(transformed_record)
    elif data_type == 'market_data':
        for record in raw_data:
            # Example transformation: ensure correct types, calculate daily change
            transformed_record = {
                'index': record.get('index'),
                'date': record.get('date'),
                'open': float(record.get('open', 0)),
                'close': float(record.get('close', 0)),
                'volume': int(record.get('volume', 0)),
                'daily_change_pct': ((float(record.get('close', 0)) - float(record.get('open', 0))) / float(record.get('open', 1))) * 100 if float(record.get('open', 1)) != 0 else 0
            }
            processed_data.append(transformed_record)
    else:
        logger.warning(f"Unknown data type '{data_type}' for transformation. Returning raw data.")
        return raw_data # Or raise an error

    logger.info(f"Transformed {len(raw_data)} records into {len(processed_data)} processed records.")
    return processed_data

def upload_processed_data_to_s3(data: Dict[str, Any], original_s3_key: str) -> str:
    """Uploads processed data to the processed S3 bucket.
    Derives the new S3 key based on the original raw data key.
    """
    try:
        # Example: raw_data/company_financials/2023-10-26/AAPL_2023-10-26_103000.json
        # -> processed_data/company_financials/2023-10-26/AAPL_2023-10-26_103000.json
        parts = original_s3_key.split('/')
        # Assuming structure: {prefix}/{data_type}/{date}/{filename}
        if len(parts) >= 4:
            data_type = parts[-3]
            date_str = parts[-2]
            filename = parts[-1]
            new_s3_key = f"{PROCESSED_S3_PREFIX}/{data_type}/{date_str}/{filename}"
        else:
            # Fallback for unexpected key structure
            new_s3_key = f"{PROCESSED_S3_PREFIX}/unclassified/{os.path.basename(original_s3_key)}"
            logger.warning(f"Unexpected S3 key structure: {original_s3_key}. Using fallback key: {new_s3_key}")

        json_data = json.dumps(data, indent=2)
        s3_client.put_object(Bucket=PROCESSED_S3_BUCKET, Key=new_s3_key, Body=json_data, ContentType='application/json')
        logger.info(f"Successfully uploaded processed data to s3://{PROCESSED_S3_BUCKET}/{new_s3_key}")
        return new_s3_key
    except ClientError as e:
        logger.error(f"Error uploading processed data to S3 key {new_s3_key}: {e}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda function handler for processing S3 events.
    Triggered when new raw data files are uploaded to the raw data S3 bucket.
    """
    logger.info(f"Received event: {json.dumps(event)}")

    for record in event['Records']:
        if 's3' not in record:
            logger.warning("Record does not contain S3 event data.")
            continue

        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        logger.info(f"Processing S3 object: s3://{bucket}/{key}")

        try:
            # 1. Load raw data
            raw_data = load_raw_data_from_s3(bucket, key)

            # 2. Determine data type from key (e.g., 'financial_data/company_financials/...')
            key_parts = key.split('/')
            if len(key_parts) > 1 and key_parts[0] == 'financial_data': # Assuming 'financial_data' is the top-level prefix
                data_type = key_parts[1]
            else:
                data_type = 'unknown'
                logger.warning(f"Could not determine data type from key: {key}. Defaulting to 'unknown'.")

            # 3. Transform data
            processed_data = transform_financial_data(raw_data, data_type)

            # 4. Upload processed data to a new S3 location
            processed_s3_key = upload_processed_data_to_s3(processed_data, key)

            logger.info(f"Successfully processed and stored data from {key} to {processed_s3_key}")

        except Exception as e:
            logger.error(f"Failed to process s3://{bucket}/{key}: {e}")
            # In a production system, consider dead-letter queues or retry mechanisms
            raise # Re-raise to indicate failure for Lambda retries

    return {
        'statusCode': 200,
        'body': json.dumps('Data processing complete.')
    }
