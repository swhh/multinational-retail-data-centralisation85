import io
import pandas as pd
import tabula
import requests
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from aws_creds import API_KEY
CREDS = 'db_creds.yaml'
PDF_PATH = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
HEADER = {"Content-Type": "application/json",
    "X-API-Key": API_KEY}
STORE_NUM_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
STORE_DETAIL_URL = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

class DataExtractor:

    def read_rds_table(self, db_conn, table_name):
        """extract the database table to a pandas DataFrame."""
        with db_conn.engine.connect() as conn:
            df = pd.read_sql_table(table_name, con=conn)
        return df
    
    def retrieve_pdf_data(self, pdf_path):
        """Takes a PDF link as an argument and returns a pandas DataFrame."""
        pdf_list = tabula.read_pdf(pdf_path, stream=True, pages='all')
        df = pd.concat(pdf_list, ignore_index=True)
        return df
    
    def list_number_of_stores(self, end_point, header_dict):
        """returns the number of stores to extract. 
        It should take in the number of stores endpoint and header dictionary as an argument."""
        response = requests.get(end_point, headers=header_dict)
        payload = response.json()
        return payload['number_stores']
    
    def retrieve_stores_data(self, end_point, header_dict):
        """takes retrieve a store endpoint as an argument and extracts all the stores from the API 
        saving them in a pandas DataFrame"""
        store_num = self.list_number_of_stores(STORE_NUM_URL, header_dict=header_dict)
        stores = []
        for store_number in range(store_num):
            response = requests.get(end_point.format(store_number=store_number), headers=header_dict)
            store_details = response.json()
            stores.append(store_details)
        return pd.DataFrame(stores)
    
    def extract_from_s3(self, s3_address, format='csv'):
        """Uses the boto3 package to download and extract the information returning a pandas DataFrame.""" 
        address_parts = s3_address.split('/')
        file_name = address_parts[-1]
        bucket_name = address_parts[-2]   
        try:
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        except NoCredentialsError:
            print("AWS credentials not found. Please configure your credentials.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchBucket':
                print("The specified bucket does not exist.")
            else:
                print("An error occurred:", e)
        file = io.BytesIO(obj['Body'].read())
        if format == 'csv':
            return pd.read_csv(file)
        if format == 'json':
            return pd.read_json
        if format == 'html':
            return pd.read_html


        


if __name__ == '__main__':
    # rws_conn = DatabaseConnector(CREDS)
    # extractor = DataExtractor() 
    # df = extractor.read_rds_table(rws_conn, 'legacy_users')
    # dc = DataCleaning()
    # clean_df = dc.clean_user_data(df)
    # sd_conn = DatabaseConnector('sales_data_db_creds.yaml')
    # sd_conn.upload_to_db(clean_df, 'dim_users')
    # print(clean_df.head())
    # print(clean_df.info())
    extractor = DataExtractor()
    result = extractor.retrieve_pdf_data(PDF_PATH)
    print(result.head())
