import logging
import configparser
import os


config = configparser.ConfigParser()
config.read_file(open("config.cfg"))

logger = logging.getLogger(__name__)

staging_schema = config.get("REDSHIFT", "DWH_STAGING_SCHEMA")
os.environ["staging_schema"] = str(staging_schema)

production_schema = config.get("REDSHIFT", "DWH_PRODUCTION_SCHEMA")
os.environ["production_schema"] = str(production_schema)

host = config.get("REDSHIFT", "DWH_HOST")
dbname = config.get("REDSHIFT", "DWH_DB")
username = config.get("REDSHIFT", "DWH_DB_USER")
password = config.get("REDSHIFT", "DWH_DB_PASSWORD")
port = config.get("REDSHIFT", "DWH_PORT")

redshift_iamrole = config.get("REDSHIFT", "DWH_IAM_ROLE_NAME")
os.environ["redshift_iamrole"] = str(redshift_iamrole)

# aws s3 config
input_bucket = config.get("BUCKET", "INPUT_BUCKET")
processing_bucket = config.get("BUCKET", "PROCESSING_BUCKET")
processed_bucket = config.get("BUCKET", "PROCESSED_BUCKET")
os.environ["processed_bucket"] = str(processed_bucket)

# aws iam config
# iam_role = config.get('IAM_ROLE', 'ARN')
KEY = config.get("AWS", "KEY")
SECRET = config.get("AWS", "SECRET")


from awsc.emr.transform import *
from awsc.s3.s3 import *
from awsc.redshift import redshift

import pandas as pd


def ingest_raw_files(s3_client):
    """
    raw data is already downloaded and move to in S3 input bucket
    """
    # process immigration data
    # fact_files = [
    #     "../../data/18-83510-I94-Data-2016/i94_jan16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_feb16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_mar16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_may16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_jun16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_jul16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_aug16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_sep16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_oct16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_nov16_sub.sas7bdat",
    #     "../../data/18-83510-I94-Data-2016/i94_dec16_sub.sas7bdat",
    # ]

    fact_files = [
        "i94_jan16_sub.sas7bdat",
        "i94_feb16_sub.sas7bdat",
        "i94_mar16_sub.sas7bdat",
        "i94_apr16_sub.sas7bdat",
        "i94_may16_sub.sas7bdat",
        "i94_jun16_sub.sas7bdat",
        "i94_jul16_sub.sas7bdat",
        "i94_aug16_sub.sas7bdat",
        "i94_sep16_sub.sas7bdat",
        "i94_oct16_sub.sas7bdat",
        "i94_nov16_sub.sas7bdat",
        "i94_dec16_sub.sas7bdat",

    ]
    preprocessed_files = []

    for fact_file in fact_files:
        preprocessed_file = f"{fact_file}.csv"
        s3_client._s3.meta.client.download_file(input_bucket, fact_file, fact_file)

        df = pd.read_sas(fact_file, "sas7bdat", encoding="ISO-8859-1")
        df.to_csv(preprocessed_file)
        s3_client.upload_files([preprocessed_file], bucket=input_bucket)
        preprocessed_files.append(preprocessed_file)

    # # store dimension data
    # dim_data = [
    #     "./data/us-cities-demographics.csv",
    #     "./data/countries_codes.txt",
    #     "./data/us_states.txt",
    #     "./data/ports.txt",
    #     "./data/modes.txt",
    #     "./data/visa_intents_types.txt",
    #     "./data/visa_types.txt",
    # ]
    # s3_client.upload_files(dim_data, bucket=input_bucket)


def main():
    """
    This is the main script to run ETL job modules:
    - Upload raw data to S3 input bucket
    - Move raw data from S3 input bucket to processing bucket
    - Initiate Spark session
    - Run transformation job in Transform module
    - Move transformed data to S3 processed bucket
    - Run datawarehouse jobs: create staging tables, insert data from S3 processed bucket to staging tables, 
    create production tables, upsert data from staging tables to production tables
    """

    print("upload files to input bucket")
    # create a s3 client
    s3_client = S3Resource(input_bucket, processing_bucket, processed_bucket, KEY, SECRET)

    # preprocess raw data provided by Udacity and push to input bucket
    ingest_raw_files(s3_client)

    # # move data from input bucket to processing bucket
    s3_client.move_data(source_bucket=input_bucket, target_bucket=processing_bucket)

    # create spark session
    spark_session = create_spark_session()

    # get files in processing bucket
    files_in_processing_bucket = s3_client.get_files_in_bucket(processing_bucket)

    # create transformer object
    transformer = Transformer(spark_session, processing_bucket, processed_bucket)

    # functions to transform each dimension file
    transformations = {
        "us-cities-demographics.csv": transformer.transform_demographics_data,
        "countries_codes.txt": transformer.transform_countries_data,
        "us_states.txt": transformer.transform_us_states_data,
        "ports.txt": transformer.transform_us_port_data,
        "modes.txt": transformer.transform_modes_data,
        "visa_intents_types.txt": transformer.transform_visa_intents_types_data,
        "visa_types.txt": transformer.transform_visa_types_data,
    }

    # transform data and move to s3 processed bucket
    for file in files_in_processing_bucket:
        # for each predefined dimension file, use the mapped function
        if file in transformations:
            transformations[file]()
        # for each fact files with format '.sas7bdat.csv'
        elif file.endswith(".sas7bdat.csv"):
            transformer.transform_immigration_data(file)

    spark_session.stop()

    # create redshift object to create Redshift databases and load data
    redshift_client = redshift.Redshift(host, dbname, username, password, port)

    print("create staging schema")
    redshift_client.create_staging_schema()
    print("drop staging tables")
    redshift_client.drop_staging_tables()
    print("create staging tables")
    redshift_client.create_staging_tables()
    print("load staging tables")
    redshift_client.load_staging_tables()
    print("create production schema")
    redshift_client.create_production_schema()
    print("drop production tables")
    redshift_client.drop_production_tables()
    print("create production tables")
    redshift_client.create_production_tables()
    print("upsert staging to production queries")
    redshift_client.upsert_staging_to_production_queries()


if __name__ == "__main__":
    main()
