import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

import requests
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FinancialDataIngestor:
    """Responsible for ingesting financial data from external APIs and storing it in S3."""

    def __init__(self, api_base_url: str, api_key: str, s3_bucket_name: str, s3_prefix: str):
        self.api_base_url = api_base_url
        self.api_key = api_key
        self.s3_bucket_name = s3_bucket_name
        self.s3_prefix = s3_prefix
        self.s3_client = boto3.client('s3')
        logger.info(f"Ingestor initialized for S3 bucket: {s3_bucket_name}/{s3_prefix}")

    def _fetch_data_from_api(self, endpoint: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fetches data from a given API endpoint."""
        headers = {'Authorization': f'Bearer {self.api_key}'}
        url = f"{self.api_base_url}/{endpoint}"
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()  # Raise an exception for HTTP errors (4xx or 5xx)
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"API request to {url} timed out after 30 seconds.")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from {url}: {e}")
            raise

    def _upload_to_s3(self, data: List[Dict[str, Any]], data_type: str, date_str: str) -> str:
        """Uploads processed data as a JSON file to S3."""
        file_name = f"{data_type}_{date_str}_{datetime.now().strftime('%H%M%S')}.json"
        s3_key = f"{self.s3_prefix}/{data_type}/{date_str}/{file_name}"
        try:
            json_data = json.dumps(data, indent=2)
            self.s3_client.put_object(Bucket=self.s3_bucket_name, Key=s3_key, Body=json_data, ContentType='application/json')
            logger.info(f"Successfully uploaded {len(data)} records to s3://{self.s3_bucket_name}/{s3_key}")
            return s3_key
        except ClientError as e:
            logger.error(f"Error uploading data to S3 key {s3_key}: {e}")
            raise
        except TypeError as e:
            logger.error(f"Serialization error for data_type {data_type}: {e}")
            raise

    def ingest_company_financials(self, symbol: str, period: str = 'annual') -> str:
        """Ingests company financial statements (e.g., income statement, balance sheet)."""
        logger.info(f"Ingesting financial statements for {symbol}, period: {period}")
        params = {'symbol': symbol, 'period': period}
        data = self._fetch_data_from_api(f'financials/{symbol}', params)
        return self._upload_to_s3(data, 'company_financials', datetime.now().strftime('%Y-%m-%d'))

    def ingest_market_data(self, market_index: str, from_date: str, to_date: str) -> str:
        """Ingests historical market index data."""
        logger.info(f"Ingesting market data for {market_index} from {from_date} to {to_date}")
        params = {'index': market_index, 'from': from_date, 'to': to_date}
        data = self._fetch_data_from_api(f'market/{market_index}/historical', params)
        return self._upload_to_s3(data, 'market_data', datetime.now().strftime('%Y-%m-%d'))

    def run_daily_ingestion(self, symbols: List[str], market_indices: List[str]):
        """Runs a daily ingestion routine for specified symbols and market indices."""
        today_str = datetime.now().strftime('%Y-%m-%d')
        yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"Starting daily ingestion for {today_str}")

        ingested_keys = []

        for symbol in symbols:
            try:
                key = self.ingest_company_financials(symbol, period='quarterly')
                ingested_keys.append(key)
            except Exception as e:
                logger.error(f"Failed to ingest financials for {symbol}: {e}")

        for market_index in market_indices:
            try:
                key = self.ingest_market_data(market_index, yesterday_str, today_str)
                ingested_keys.append(key)
            except Exception as e:
                logger.error(f"Failed to ingest market data for {market_index}: {e}")
        
        logger.info(f"Daily ingestion completed. Total {len(ingested_keys)} files ingested.")
        return ingested_keys

# Example Usage (typically orchestrated by a workflow manager like Airflow/Step Functions)
if __name__ == '__main__':
    # Environment variables for sensitive info and configuration
    API_BASE_URL = os.environ.get('FINANCIAL_API_BASE_URL', 'https://api.example.com/v1')
    API_KEY = os.environ.get('FINANCIAL_API_KEY', 'your_financial_api_key') # DO NOT hardcode in production
    S3_BUCKET_NAME = os.environ.get('RAW_DATA_S3_BUCKET', 'cb-global-raw-financial-data')
    S3_PREFIX = os.environ.get('S3_RAW_DATA_PREFIX', 'financial_data')

    ingestor = FinancialDataIngestor(
        api_base_url=API_BASE_URL,
        api_key=API_KEY,
        s3_bucket_name=S3_BUCKET_NAME,
        s3_prefix=S3_PREFIX
    )

    # Define data sources to ingest
    target_companies = ['AAPL', 'MSFT', 'GOOGL']
    target_market_indices = ['SPX', 'NDX']

    try:
        # Simulate a daily ingestion run
        ingested_files = ingestor.run_daily_ingestion(target_companies, target_market_indices)
        logger.info(f"Successfully completed ingestion. Ingested files: {ingested_files}")
    except Exception as e:
        logger.critical(f"Critical error during daily ingestion: {e}")
