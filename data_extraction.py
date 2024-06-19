import pandas as pd
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
CREDS = 'db_creds.yaml'

class DataExtractor:

    def read_rds_table(self, db_conn, table_name):
        """extract the database table to a pandas DataFrame."""
        with db_conn.engine.connect() as conn:
            df = pd.read_sql_table(table_name, con=conn)
        return df
    


if __name__ == '__main__':
    rws_conn = DatabaseConnector(CREDS)
    extractor = DataExtractor() 
    df = extractor.read_rds_table(rws_conn, 'legacy_users')
    dc = DataCleaning()
    clean_df = dc.clean_user_data(df)
    sd_conn = DatabaseConnector('sales_data_db_creds.yaml')
    sd_conn.upload_to_db(clean_df, 'dim_users')
    print(clean_df.head())
    print(clean_df.info())