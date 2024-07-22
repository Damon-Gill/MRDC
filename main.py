from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

if __name__ == "__main__":

    db_connector = DatabaseConnector()
    extractor = DataExtractor()
    cleaner = DataCleaning()

    #api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    #number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    #Store_details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    #number_of_stores = extractor.list_number_of_stores(number_of_stores_endpoint)
    #stores_data_df = extractor.retrieve_stores_data(store_details_endpoint, number_of_stores)

    #s3_address = 's3://data-handling-public/products.csv'
    #products_df = extractor.extract_from_s3(s3_address)

    #cleaned_products_data = cleaner.convert_product_weights(products_df)

    #db_connector.upload_to_db(cleaned_products_data, 'dim_products')

    #tables_list = db_connector.list_db_tables()
    #print(tables_list)

    #orders_df = extractor.read_rds_table(db_connector, table_name='orders_table')

    #cleaned_orders_df = cleaner.clean_orders_data(orders_df)
    #db_connector.upload_to_db(cleaned_orders_df, 'orders_table')

    json_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_df = extractor.extract_from_json(json_url)

    cleaned_date_df = cleaner.clean_date_details(date_df)
    db_connector.upload_to_db(cleaned_date_df, 'dim_date_times')