import os
import glob
import datetime as dt
import json

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    IntegerType,
    FloatType,
    TimestampType,
    LongType,
    DateType,
    NullType,
    StructType,
    StructField,
)
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import lower
from pyspark.sql.functions import udf, col

# datetime functions
def to_datetime(x):
    try:
        start = dt.datetime(1960, 1, 1).date()
        return start + dt.timedelta(days=int(x))
    except:
        return None


udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())


def to_datetimefrstr(x):
    try:
        return dt.datetime.strptime(x, "%m%d%Y")
    except:
        return None


udf_to_datetimefrstr = udf(lambda x: to_datetimefrstr(x), DateType())


def cdf_Ymd_to_mmddYYYY(x):
    try:
        return dt.datetime.strptime(x, "%Y%m%d")
    except:
        return None


udf_cdf_Ymd_to_mmddYYYY = udf(lambda x: cdf_Ymd_to_mmddYYYY(x), DateType())


def cdf_mdY_to_mmddYYYY(x):
    try:
        return dt.datetime.strptime(x, "%m%d%Y")
    except:
        return None


udf_cdf_mdY_to_mmddYYYY = udf(lambda x: cdf_mdY_to_mmddYYYY(x), DateType())

remove_single_quote = udf(lambda x: x.replace("'", "").strip(), StringType())


def create_spark_session():
    """
    Create spark session
    Parameters: None
    Returns: spark session
    """
    spark_session = (
        SparkSession.builder.config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")
        .appName("Capstone Project US Immigration")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark_session


class Transformer:
    """
    This class cleans and transforms raw data before data warehouse
    - Read in raw files
    - Name colums accordingly
    - Change data types
    - Remove whitespace, single quote, etc.
    - Format datetime
    - Remove dupplicates, invalid data
    - Write to csv after transforming
    
    """

    def __init__(self, spark_session, processing_bucket, processed_bucket):
        self._spark_session = spark_session
        self._processing_bucket = f"s3a://{processing_bucket}"
        self._processed_bucket = f"s3a://{processed_bucket}"

    def transform_immigration_data(self, file):

        print(f"function transform_immigration_data, {file}")

        # read data
        # immigration_df = self._spark_session.read.format('com.github.saurfang.sas.spark').load(filepath)
        # immigration_df = self._spark_session.read.load(f"{self._processing_bucket}/{file}",header='true')

        print("reading file")
        immigration_df = self._spark_session.read.load(
            f"{self._processing_bucket}/{file}", format="csv", header="true", sep=","
        )

        # change column name and data type
        immigration_df = (
            immigration_df.withColumn("i94yr", immigration_df["i94yr"].cast(IntegerType()))
            .withColumn("i94mon", immigration_df["i94mon"].cast(IntegerType()))
            .withColumn("arrdate", immigration_df["arrdate"].cast(IntegerType()))
            .withColumn("depdate", immigration_df["depdate"].cast(IntegerType()))
            .withColumn("i94bir", immigration_df["i94bir"].cast(IntegerType()))
            .withColumn("count", immigration_df["count"].cast(IntegerType()))
            .withColumn("biryear", immigration_df["biryear"].cast(IntegerType()))
            .withColumn("i94cit", immigration_df["i94cit"].cast(IntegerType()).cast(StringType()))
            .withColumn("i94res", immigration_df["i94res"].cast(IntegerType()).cast(StringType()))
            .withColumn("i94mode", immigration_df["i94mode"].cast(IntegerType()).cast(StringType()))
            .withColumn("i94visa", immigration_df["i94visa"].cast(IntegerType()).cast(StringType()))
        )

        # tranform date columns to datetime format
        immigration_df = (
            immigration_df.withColumn("arrdate", udf_to_datetime_sas(immigration_df.arrdate))
            .withColumn("depdate", udf_to_datetime_sas(immigration_df.depdate))
            .withColumn("dtadfile", udf_cdf_Ymd_to_mmddYYYY(immigration_df.dtadfile))
            .withColumn("dtaddto", udf_cdf_mdY_to_mmddYYYY(immigration_df.dtaddto))
        )

        immigration_df = immigration_df.withColumn(
            "arrdate", when(col("arrdate") == "1960-01-01", lit(None)).otherwise(col("arrdate"))
        )
        immigration_df = immigration_df.withColumn(
            "depdate", when(col("depdate") == "1960-01-01", lit(None)).otherwise(col("depdate"))
        )

        # Departure date cannot smaller than Arrival date and Departure date can be null
        immigration_df = immigration_df.filter(
            ~(immigration_df.arrdate > immigration_df.depdate) | (immigration_df.depdate.isNull())
        )

        # gender
        immigration_df = immigration_df.withColumn("gender", when(col("gender") == "X", "NA").otherwise(col("gender")))

        # choose columns to keep
        immigration_df = immigration_df[
            [
                "cicid",
                "i94yr",
                "i94mon",
                "i94cit",
                "i94res",
                "i94port",
                "arrdate",
                "i94mode",
                "i94addr",
                "depdate",
                "i94bir",
                "i94visa",
                "count",
                "dtadfile",
                "visapost",
                "occup",
                "entdepa",
                "entdepd",
                "entdepu",
                "matflag",
                "biryear",
                "dtaddto",
                "gender",
                "insnum",
                "airline",
                "admnum",
                "fltno",
                "visatype",
            ]
        ]

        # write to csv
        # immigration_df.to_csv(f"{self._processed_bucket}/immigration_df.csv", encoding='utf-8', index=False)
        print("save file")
        #         immigration_df.coalesce(1).write.save(path=f"{self._processed_bucket}/fact_immigration_events", format='csv', mode='append', sep=',')
        #         immigration_df.write.csv(path=f"{self._processed_bucket}/fact_immigration_events/{file}", sep = ',', mode='overwrite', header=True)
        immigration_df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option(
            "delimiter", "|"
        ).option("quote", "\u0000").save(f"{self._processed_bucket}/fact_immigration_events/{file}")

    def transform_countries_data(self):

        # udf
        clean_mexico = udf(
            lambda x: x.replace("MEXICO Air Sea, and Not Reported (I-94, no land arrivals)", "MEXICO"), StringType()
        )

        # read file
        countries_df = self._spark_session.read.csv(f"{self._processing_bucket}/countries_codes.txt", sep="=")

        # change column name and data type
        countries_df = countries_df.withColumn("country_code", countries_df["_c0"].cast(StringType())).withColumn(
            "country_name", countries_df["_c1"].cast(StringType())
        )

        # apply udf
        countries_df = countries_df.withColumn("country_name", remove_single_quote(countries_df.country_name))
        countries_df = countries_df.withColumn("country_name", clean_mexico(countries_df.country_name))

        # remove No Country/ invalid codes / Collapsed /
        countries_df = countries_df.where(~lower(countries_df.country_name).like("%invalid%"))
        countries_df = countries_df.where(~lower(countries_df.country_name).like("%no country%"))
        countries_df = countries_df.where(~lower(countries_df.country_name).like("%collapsed%"))

        # choose columns to keep
        countries_df = countries_df[["country_code", "country_name"]]

        # write to csv
        countries_df.write.csv(path=f"{self._processed_bucket}/dim_countries", mode="overwrite", header=True)

    def transform_us_states_data(self):

        # read file
        us_states_df = self._spark_session.read.load(f"{self._processing_bucket}/us_states.txt", format="csv", sep="=")

        # change column name and data type
        us_states_df = us_states_df.withColumn("state_code", us_states_df["_c0"].cast(StringType())).withColumn(
            "state_name", us_states_df["_c1"].cast(StringType())
        )

        # remove single quote
        us_states_df = us_states_df.withColumn("state_code", remove_single_quote(us_states_df.state_code))
        us_states_df = us_states_df.withColumn("state_name", remove_single_quote(us_states_df.state_name))

        # choose columns to keep
        us_states_df = us_states_df[["state_code", "state_name"]]

        # write to csv
        us_states_df.write.csv(path=f"{self._processed_bucket}/dim_states", mode="overwrite", header=True)

    def transform_demographics_data(self):

        # read file
        demographics_df = self._spark_session.read.load(
            f"{self._processing_bucket}/us-cities-demographics.csv", format="csv", header="true", sep=";"
        )

        # change column name and data type
        demographics_df = (
            demographics_df.withColumn("city", demographics_df["City"].cast(StringType()))
            .withColumn("median_age", demographics_df["Median Age"].cast(FloatType()))
            .withColumn("male_pop", demographics_df["Male Population"].cast(IntegerType()))
            .withColumn("female_pop", demographics_df["Female Population"].cast(IntegerType()))
            .withColumn("total_pop", demographics_df["Total Population"].cast(IntegerType()))
            .withColumn("num_vets", demographics_df["Number of Veterans"].cast(IntegerType()))
            .withColumn("foreign_born", demographics_df["Foreign-born"].cast(IntegerType()))
            .withColumn("avg_household_size", demographics_df["Average Household Size"].cast(FloatType()))
            .withColumn("state_code", demographics_df["State Code"].cast(StringType()))
            .withColumn("race", demographics_df["Race"].cast(StringType()))
            .withColumn("count", demographics_df["Count"].cast(IntegerType()))
        )

        # choose columns to keep
        demographics_summary_df = demographics_df[
            [
                "state_code",
                "city",
                "median_age",
                "male_pop",
                "female_pop",
                "total_pop",
                "num_vets",
                "foreign_born",
                "avg_household_size",
            ]
        ]
        # choose columns to keep
        demographics_race_df = demographics_df[["state_code", "city", "race", "count"]]

        # remove duplicates
        demographics_summary_df = demographics_summary_df.distinct()

        # write to csv
        demographics_summary_df.write.csv(
            path=f"{self._processed_bucket}/dim_cities_demographics_summary", mode="overwrite", header=True
        )
        demographics_race_df.write.csv(
            path=f"{self._processed_bucket}/dim_cities_demographics_race", mode="overwrite", header=True
        )

    def transform_us_port_data(self):

        # read file
        ports_df = self._spark_session.read.load(f"{self._processing_bucket}/ports.txt", format="csv", sep="=")
        ports_df = ports_df.withColumn("port_code", ports_df["_c0"].cast(StringType())).withColumn(
            "port_name", ports_df["_c1"].cast(StringType())
        )

        # strip '' from data
        ports_df = ports_df.withColumn("port_code", remove_single_quote(ports_df.port_code))
        ports_df = ports_df.withColumn("port_name", remove_single_quote(ports_df.port_name))

        # remove invalid and not us ports
        ports_df = ports_df.where(~lower(ports_df.port_name).like("%collapsed%"))
        ports_df = ports_df.where(~lower(ports_df.port_name).like("%no port%"))
        ports_df = ports_df.where(~lower(ports_df.port_name).like("%unknown%"))
        ports_df = ports_df.where(~lower(ports_df.port_name).like("%identifi%"))

        # separate port_name to port_city and port_state
        split_col = pyspark.sql.functions.split(ports_df["port_name"], ",")
        ports_df = ports_df.withColumn("port_city", split_col.getItem(0))
        ports_df = ports_df.withColumn("port_state", split_col.getItem(1))
        ports_df = ports_df.withColumn("port_state", trim(col("port_state")))
        ports_df = ports_df.where(pyspark.sql.functions.length(ports_df.port_state) == 2)

        ports_df = ports_df[["port_code", "port_city", "port_state"]]

        # write to csv
        ports_df.write.csv(path=f"{self._processed_bucket}/dim_ports", mode="overwrite", header=True)

    def transform_modes_data(self):

        # read file
        modes_df = self._spark_session.read.load(f"{self._processing_bucket}/modes.txt", format="csv", sep="=")

        # change column name and data type
        modes_df = modes_df.withColumn("arrival_mode_code", modes_df["_c0"].cast(StringType())).withColumn(
            "arrival_mode_type", modes_df["_c1"].cast(StringType())
        )

        # strip whitespace and remove single_quote
        modes_df = modes_df.withColumn("arrival_mode_type", remove_single_quote(modes_df.arrival_mode_type))
        modes_df = modes_df.withColumn("arrival_mode_code", trim(col("arrival_mode_code"))).withColumn(
            "arrival_mode_type", trim(col("arrival_mode_type"))
        )
        modes_df = modes_df[["arrival_mode_code", "arrival_mode_type"]]

        # write to csv
        modes_df.write.csv(path=f"{self._processed_bucket}/dim_arrival_modes", mode="overwrite", header=True)

    def transform_visa_intents_types_data(self):

        # read file
        visa_intents_types_df = self._spark_session.read.load(
            f"{self._processing_bucket}/visa_intents_types.txt", format="csv", sep="="
        )

        # change column name and data type
        visa_intents_types_df = visa_intents_types_df.withColumn(
            "visa_intents_code", visa_intents_types_df["_c0"].cast(StringType())
        ).withColumn("visa_intents_type", visa_intents_types_df["_c1"].cast(StringType()))

        #  strip '' from data
        visa_intents_types_df = visa_intents_types_df.withColumn(
            "visa_intents_code", trim(col("visa_intents_code"))
        ).withColumn("visa_intents_type", trim(col("visa_intents_type")))
        visa_intents_types_df = visa_intents_types_df[["visa_intents_code", "visa_intents_type"]]

        # write to csv
        visa_intents_types_df.write.csv(
            path=f"{self._processed_bucket}/dim_visa_intents_types", mode="overwrite", header=True
        )

    def transform_visa_types_data(self):

        # read file
        visa_types_df = self._spark_session.read.load(
            f"{self._processing_bucket}/visa_types.txt", format="csv", sep="|"
        )

        # change column name and data type
        visa_types_df = visa_types_df.withColumn("visa_code", visa_types_df["_c0"].cast(StringType())).withColumn(
            "visa_type", visa_types_df["_c1"].cast(StringType())
        )

        #  strip '' from data
        visa_types_df = visa_types_df.withColumn("visa_code", trim(col("visa_code"))).withColumn(
            "visa_type", trim(col("visa_type"))
        )
        visa_types_df = visa_types_df[["visa_code", "visa_type"]]

        # write to csv
        visa_types_df.write.csv(path=f"{self._processed_bucket}/dim_visa_types", mode="overwrite", header=True)

