import psycopg2
from pathlib import Path
from .staging_queries import create_staging_schema, staging_drop_US_immigration_tables,staging_create_US_immigration_tables,staging_copy_tables
from .production_queries import create_production_schema, production_drop_US_immigration_tables,production_create_US_immigration_tables
from .upsert_staging_to_production import upsert_staging_to_production_queries
import logging

class Redshift:
    """
    This class implement warehouse jobs by executing queries in production_queries.py, staging_queries.py, and upsert_staging_to_production.py
    """
    def __init__(self,host,dbname,username,password,port):
        self._host = host
        self._dbname = dbname
        self._username = username
        self._password = password
        self._port = port
        self._conn = psycopg2.connect(f"host={host} dbname={dbname} user={username} password={password} port={port}")
        self._cur = self._conn.cursor()
    
    def execute_queries(self, queries):
        for query in queries:
            print(query)
            logging.debug(f"Executing Query : {query}")
            self._cur.execute(query)
            self._conn.commit()                                  

    def create_staging_schema(self):
        logging.debug("create staging schema")
        self.execute_queries([create_staging_schema])
    
    def drop_staging_tables(self):
        logging.debug("drop staging tables")
        self.execute_queries(staging_drop_US_immigration_tables)
                                      
    def create_staging_tables(self):
        logging.debug("create staging tables")
        self.execute_queries(staging_create_US_immigration_tables)   
                                      
    def load_staging_tables(self):
        logging.debug("load staging tables")
        self.execute_queries(staging_copy_tables) 
                                  
    def upsert_staging_to_production_queries(self):
        logging.debug("upsert staging to production queries")
        self.execute_queries(upsert_staging_to_production_queries)                                         
                                      
    def create_production_schema(self):
        logging.debug("create production schema")
        self.execute_queries([create_production_schema])
    
    def drop_production_tables(self):
        logging.debug("drop production tables")
        self.execute_queries(production_drop_US_immigration_tables)
                                      
    def create_production_tables(self):
        logging.debug("create production tables")
        self.execute_queries(production_create_US_immigration_tables) 