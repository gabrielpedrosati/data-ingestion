# Import libraries
import os
import pymysql
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


# Import env vars
load_dotenv(dotenv_path=".env")

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")


class Ingestion:
    """This class defines the Ingestion process.

    Ingest local csv file into database.

    Attributes:
        host (string): database hostname.
        database (string): database name.
        user (string): database user.
        password (string): database password.
        port (int): database port.
        engine (object): database connection object.
        df (object): pandas dataframe.

    Methods:
        connect_database(self):
            Return database connection object.
        read_source(self, filename, delimiter):
            Return a pandas dataframe.
        load_to_database(self, table):
            Load pandas dataframe to database.
        close_database_conn(self):
            Terminate the database connection.
    """

    def __init__(self, host, database, user, password, port):
        """Create an instance of Ingestion and connect to database.
        
        Arguments:
            host (string): database hostname.
            database (string): database name.
            user (string): database user.
            password (string): database password.
            port (int): database port.
        """

        try:
            self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
            print("Connected to the database!")
        
        except Exception as e:
            print("Connection failed! Exception: ", e)
            
    def read_csv_file(self, filename, delimiter):
        """Create a pandas dataframe from a csv file and store in self.df.

        Attributes:
            filename (string): csv file path.
            delimiter (string): csv file delimiter.
        """

        try:
            self.df = pd.read_csv(filename, delimiter=delimiter, encoding="utf-8")
        
        except Exception as e:
            print("Failed reading file! Exception: ", e)
            
    def load_to_database(self, table):
        """Load dataframe into database.

        Attribute:
            table (string): destination table name.        
        """

        try:
            self.df.to_sql(name=table, con=self.engine, if_exists="append", index=False)
            print("Loaded successfully!")
        
        except Exception as e:
            print("Load failed! Exception: ", e)
            
    def close_connection(self):
        """Close database connection"""

        self.engine.dispose()
      

if __name__ == "__main__":
    
    ingest = Ingestion(host, database, user, password, port)
        
    ingest.read_csv_file("./ARQUIVOS_GENERICOS/STATIONS.csv", ",")
    
    ingest.load_to_database("stations")
    
    ingest.close_connection()