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