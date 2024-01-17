import yaml
import sqlalchemy


class DatabaseConnector:
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            creds = yaml.safe_load(file)
        return creds

    def init_db_engine(self):
        creds = self.read_db_creds()

        database_type = 'postgresql'
        host = creds['RDS_HOST']
        username = creds['RDS_USER']
        password = creds['RDS_PASSWORD']
        database = creds['RDS_DATABASE']
        port = creds['RDS_PORT']

        db_conn_url = f"{database_type}://{username}:{password}@{host}:{port}/{database}"

        engine = sqlalchemy.create_engine(db_conn_url)
        return engine

    def list_db_tables(self):
        engine = self.init_db_engine()
        with engine.connect() as conn:
            metadata = sqlalchemy.MetaData()
            metadata.reflect(bind=conn)

            tables = metadata.tables.keys()

            return tables

    def upload_to_db(self, df, table_name):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = '1234'
        DATABASE = 'sales_data'
        PORT = 5432
        local_engine = sqlalchemy.create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        df.to_sql(table_name, local_engine, if_exists="replace")


if __name__ == '__main__':
    print("Tables in Database:", DatabaseConnector().list_db_tables())

    from data_cleaning import DataCleaning

    DatabaseConnector().upload_to_db(DataCleaning().clean_user_data(), "dim_users")
