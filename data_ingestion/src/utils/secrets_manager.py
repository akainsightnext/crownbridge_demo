import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

def get_secret(secret_arn: str) -> str:
    """Retrieves a secret from AWS Secrets Manager using its ARN."""
    region_name = os.environ.get('AWS_REGION', 'us-east-1') # Default to us-east-1 if not set

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_arn)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            logger.error(f"Secrets Manager decryption failure for {secret_arn}: {e}")
            raise
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            logger.error(f"Secrets Manager internal service error for {secret_arn}: {e}")
            raise
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            logger.error(f"Secrets Manager invalid parameter for {secret_arn}: {e}")
            raise
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            logger.error(f"Secrets Manager invalid request for {secret_arn}: {e}")
            raise
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            logger.error(f"Secrets Manager resource not found: {secret_arn}: {e}")
            raise
        else:
            logger.error(f"An unexpected Secrets Manager error occurred for {secret_arn}: {e}")
            raise
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            return get_secret_value_response['SecretString']
        else:
            # Handle binary secrets if needed, though API keys are typically strings
            logger.warning(f"Secret {secret_arn} is binary. Returning base64 encoded string.")
            return get_secret_value_response['SecretBinary'].decode('utf-8')


def get_api_key_from_secret(secret_arn: str, key_name: str = 'api_key') -> str:
    """Retrieves a specific API key from a JSON secret stored in Secrets Manager."""
    secret_string = get_secret(secret_arn)
    try:
        secret_dict = json.loads(secret_string)
        api_key = secret_dict.get(key_name)
        if not api_key:
            raise ValueError(f"Key '{key_name}' not found in secret JSON for {secret_arn}")
        return api_key
    except json.JSONDecodeError as e:
        logger.error(f"Secret string for {secret_arn} is not valid JSON: {e}")
        raise ValueError(f"Secret string is not valid JSON for {secret_arn}") from e
    except Exception as e:
        logger.error(f"Error extracting API key from secret {secret_arn}: {e}")
        raise

# Example usage (for local testing or other scripts, not directly in Lambda handler)
if __name__ == '__main__':
    # This part would typically be used in the ingestion lambda, but for demonstration:
    # In a real Lambda, the secret ARN would come from environment variables.
    # For local testing, you might mock this or set an environment variable.
    # os.environ['FINANCIAL_API_KEY_SECRET_ARN'] = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:MyFinancialApiKey-xxxxxx'
    # os.environ['AWS_REGION'] = 'us-east-1'

    mock_secret_arn = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:test/api/key-abcd'
    # Simulate a secret in Secrets Manager
    # client.create_secret(Name='test/api/key-abcd', SecretString='{"api_key": "mock_api_key_123"}')

    try:
        # This will fail if the secret doesn't exist or permissions are wrong locally
        # For a successful run, ensure you have AWS credentials configured and the secret exists.
        # financial_api_key = get_api_key_from_secret(os.environ['FINANCIAL_API_KEY_SECRET_ARN'])
        # print(f"Successfully retrieved API Key (first 5 chars): {financial_api_key[:5]}...")
        print("Run this script within an environment where AWS credentials and a valid secret ARN are available.")
        print("Or, mock 'boto3.client('secretsmanager').get_secret_value' for local testing.")

    except Exception as e:
        print(f"Could not retrieve secret: {e}")
