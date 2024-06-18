import pandas as pd
from database_utils import DatabaseConnector
CREDS = 'db_creds.yaml'

class DataExtractor:

    def read_rds_table(self, db_conn, table_name):
        """extract the database table to a pandas DataFrame."""
        with db_conn.engine.connect() as conn:
            df = pd.read_sql_table(table_name, db_conn.engine)
        return df
    


if __name__ == '__main__':
    conn = DatabaseConnector(CREDS)
    extractor = DataExtractor() 
    df = extractor.read_rds_table(conn, 'legacy_users')
    # print(df.head())
    # print(df.info())