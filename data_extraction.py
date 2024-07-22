import pandas as pd
import boto3
import requests
import tabula
import io

class DataExtractor:

    def __init__(self):
        pass

    def extract_from_csv(self, file_path):

        return pd.read_csv(file_path)

    def extract_from_api(self, api_url):
        response = requests.get(api_url)

        return pd.DataFrame(response.json())

    def extract_from_s3(self, s3_address):
        s3_bucket, s3_key = s3_address.replace("s3://", "").split("/", 1)
        s3_client = boto3.client('s3', region_name='eu-west-2')
        
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        
        csv_content = response['Body'].read().decode('utf-8')
        products_df = pd.read_csv(io.StringIO(csv_content))
        
        return products_df
    
    def extract_from_json(self, url):
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        return df

    def read_rds_table(self, db_connector, table_name):
        engine = db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"

        return pd.read_sql(query, con=engine)
    
    def retrieve_pdf_data(self, pdf_url):
        df = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True)
        combined_df = pd.concat(df, ignore_index=True)

        return combined_df
    
    def list_number_of_stores(self, number_of_stores_endpoint):
        response = requests.get(number_of_stores_endpoint, headers=self.headers)
        if response.status_code == 200:
            return response.json().get('number_stores')
        else:
            response.raise_for_status()

    def retrieve_stores_data(self, store_details_endpoint, number_of_stores):
        stores_data = []
        for store_number in range(1, number_of_stores):
            response = requests.get(store_details_endpoint.format(store_number=store_number), headers=self.headers)
            if response.status_code == 200:
                stores_data.append(response.json())
            else:
                response.raise_for_status()
        return pd.DataFrame(stores_data)
