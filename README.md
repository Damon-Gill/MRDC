# Multinational Retail Data Centralisation

The sales data at a multinational company are currently spread across multiple sources, This project looks at centralising all current data from the different sources and hosting it in one single source of truth, this allows querying the database to get up-to-date metrics for the business.

## Milestone 1

- This milestone was completed by creating the repository in github and also creating the working environment within visual studio code.

## Milestone 2

- A database was set up in PgAdmin4 in order to begin uploading data frim different sources, this will be the database for the single source of truth for the entirety of the data. The serve has been named "sales-data" for the user to easily identify the contents without the requirement to query the database.
- 3 project classes were created: "database_utils.py", "data_extraction.py", "data_cleaning.py", which wil be used to: connect to an existing database & upload to the new database, extract data from various different sources and "clean" the data of any erroneous entries respectively.
- Within these classes, data was extracted, cleaned and uploaded to the new database. Sources where data was obtained from were: csv files (from an s3 bucket), api's, json files (from an s3 bucket), pdf's (from an s3 bucket), databases. The classes code can be seen below (some of the code has been commented out as each section was ran one by one and was removed when the data had already been uploaded to the new database).

### database_utils.py

```python
import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name):
        engine2 = create_engine(f"postgresql://DamonGill:Damon-1995@sales-data.cjed6pewrsts.eu-west-2.rds.amazonaws.com:5432/postgres")
        df.to_sql(table_name, con=engine2, if_exists='replace', index=False)
```
### database_extraction.py
```python
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
```

### database_cleaning.py
```python
import pandas as pd
import re

class DataCleaning:
    def __init__(self):
        pass
    
    def clean_user_data(self, df):
        df = df.dropna(inplace=True)  # Remove rows with NULL values
        #df = df.drop_duplicates()
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['continent'] = df['continent'].apply(lambda x: 'Europe' if 'Europe' in x else ('America' if 'America' in x else x))

        return df
    
    def convert_weight(self, weight):
        if isinstance(weight, str):
            weight = weight.lower().strip()
            # Check for multiplication patterns like '12 x 100'
            match = re.match(r'(\d+)\s*x\s*(\d+)', weight)
            if match:
                try:
                    return (float(match.group(1)) * float(match.group(2))) / 1000
                except ValueError:
                    print(f"Failed to convert weight: {weight}")
                    return None
            elif 'kg' in weight:
                try:
                    return float(weight.replace('kg', '').strip())
                except ValueError:
                    print(f"Failed to convert weight: {weight}")
                    return None
            elif 'g' in weight:
                try:
                    return float(weight.replace('g', '').strip()) / 1000
                except ValueError:
                    print(f"Failed to convert weight: {weight}")
                    return None
            else:
                try:
                    return float(weight)
                except ValueError:
                    print(f"Failed to convert weight: {weight}")
                    return None
        elif isinstance(weight, (int, float)):
            return float(weight)
        else:
            return None  # or return a default value like 0

    def convert_product_weights(self, products_df):
        products_df['weight_kg'] = products_df['weight'].apply(self.convert_weight)
        return products_df
    
    def clean_orders_data (self, df):
        columns_to_remove = ['first_name', 'last_name', '1']
        df_cleaned = df.drop(columns=columns_to_remove, errors='ignore')
        return df_cleaned
    
    def clean_date_details(self, date_df):
        #date_df = date_df.dropna(inplace=True)
        return date_df
```

## Milestone 3

- Data types were corrected to show the true types for each table, below is an example from the main data table (orders_table).
- Primary keys were then made in all the dimension table and similarly, foreign keys were made in the data table in order to link all the dimensions and finalise the star schema.
- During this milestone it was noted that the cleaning previously carried out in milestone 2 was not sufficient, therefore further cleaning was carried out in order to correct the data types.

![Image of the orders table keys](https://raw.githubusercontent.com/Damon-Gill/MRDC/refs/heads/main/Orders%20Table.png)

![Image of the foreign keys in the orders_table](https://raw.githubusercontent.com/Damon-Gill/MRDC/refs/heads/main/Orders%20Table%20keys.png)


## Milestone 4

- Now the schemas have been set up, the data is ready toi be queried to find essential metrics.
- The number of stores in specific countries/locations, sales, staff headcount etc can all be queried.
- Below is an example of a query highlighting the average time taken per sale for each year.

```python

WITH sales_with_time AS (
    SELECT
        d.year,
        TO_TIMESTAMP(
            CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp), 
            'YYYY-MM-DD HH24:MI:SS'
        ) AS sale_time,
        LEAD(
            TO_TIMESTAMP(
                CONCAT(d.year, '-', LPAD(d.month::text, 2, '0'), '-', LPAD(d.day::text, 2, '0'), ' ', d.timestamp), 
                'YYYY-MM-DD HH24:MI:SS'
            )
        ) OVER (PARTITION BY d.year ORDER BY d.year, d.month, d.day, d.timestamp) AS next_sale_time
    FROM
        orders_table o
    JOIN
        dim_date_times d ON o.date_uuid = d.date_uuid
    WHERE
        d.timestamp IS NOT NULL
),
time_differences AS (
    SELECT
        year,
        EXTRACT(EPOCH FROM (next_sale_time - sale_time)) AS time_diff_in_seconds
    FROM
        sales_with_time
    WHERE
        next_sale_time IS NOT NULL
)
SELECT
    year,
    CONCAT(
        '"hours": ', FLOOR(AVG(time_diff_in_seconds) / 3600), ', ',
        '"minutes": ', FLOOR((AVG(time_diff_in_seconds) % 3600) / 60), ', ',
        '"seconds": ', FLOOR(AVG(time_diff_in_seconds) % 60), ', ',
        '"milliseconds": ', ROUND((AVG(time_diff_in_seconds) * 1000) % 1000)
    ) AS actual_time_taken
FROM
    time_differences
GROUP BY
    year
ORDER BY
    year;
```
