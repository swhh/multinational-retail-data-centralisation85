import yaml
from sqlalchemy import create_engine, inspect



ENGINE_ADDRESS = "{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

class DatabaseConnector:

    def __init__(self, yaml_file):
        creds = self.read_db_creds(yaml_file)
        self.engine = self.init_db_engine(creds)

    @staticmethod
    def read_db_creds(yaml_file):
        """Read dc_creds.yaml and return creds dictionary"""
        with open(yaml_file) as f:
            creds_dict = yaml.safe_load(f)
        return creds_dict
    
    @staticmethod
    def init_db_engine(creds_dict):
        """read the credentials from read_db_creds 
        and initialise and return an sqlalchemy database engine"""
        engine_address = ENGINE_ADDRESS.format(**creds_dict)
        engine = create_engine(engine_address)
        return engine
    
    @staticmethod
    def list_tables(engine):
        """list all the tables in the database"""
        inspector = inspect(engine)
        return inspector.get_table_names()
    
    def upload_to_db(self, df, table_name):
        """take in a Pandas DataFrame and table name to upload to as an argument."""
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            df.to_sql(table_name, self.engine, if_exists='replace', index=False)

    
    
if __name__ == '__main__':
    db_conn = DatabaseConnector('sales_data_db_creds.yaml')
    print(db_conn.list_tables(db_conn.engine))
    