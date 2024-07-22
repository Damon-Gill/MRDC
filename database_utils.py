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