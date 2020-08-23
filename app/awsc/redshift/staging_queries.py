import configparser
from pathlib import Path
import os

US_immigration_schema = os.environ["staging_schema"]
staging_schema = os.environ["staging_schema"]
processed_bucket = f"s3://{os.environ['processed_bucket']}"
redshift_iamrole = os.environ["redshift_iamrole"]

create_staging_schema = f"CREATE SCHEMA IF NOT EXISTS {US_immigration_schema};"

drop_fact_immigration_events = f"DROP TABLE IF EXISTS {US_immigration_schema}.fact_immigration_events;"
drop_dim_countries = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_countries;"
drop_dim_states = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_states;"
drop_dim_cities_demographics_summary = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_cities_demographics_summary;"
drop_dim_cities_demographics_race = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_cities_demographics_race;"
drop_dim_ports = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_ports;"
drop_dim_arrival_modes = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_arrival_modes;"
drop_dim_visa_intents_types = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_visa_intents_types;"
drop_dim_visa_types = f"DROP TABLE IF EXISTS {US_immigration_schema}.dim_visa_types;"


create_fact_immigration_events = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.fact_immigration_events
(
    immigration_id integer IDENTITY(0,1) CONSTRAINT fact_immigration_events_pkey primary key,
    cicid varchar not null,
    i94yr integer not null,
    i94mon integer not null,
    i94cit varchar(3) REFERENCES {US_immigration_schema}.dim_countries(country_code),
    i94res varchar(3),
    i94port varchar(3) REFERENCES {US_immigration_schema}.dim_ports(port_code),
    arrdate varchar,
    i94mode varchar REFERENCES {US_immigration_schema}.dim_arrival_modes(arrival_mode_code),
    i94addr varchar,
    depdate varchar,
    i94bir varchar,
    i94visa varchar REFERENCES {US_immigration_schema}.dim_visa_intents_types(visa_intents_code),
    count integer,
    dtadfile varchar,
    visapost varchar,
    occup varchar,
    entdepa varchar,
    entdepd varchar,
    entdepu varchar,
    matflag varchar,
    biryear varchar,
    dtaddto varchar,
    gender varchar,
    insnum varchar,
    airline varchar,
    admnum varchar,
    fltno varchar,
    visatype varchar
)
;
"""


create_dim_countries = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_countries
(
    country_code varchar not null constraint dim_countries_pkey primary key,
    country_name varchar not null
)
;
"""


create_dim_states = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_states
(
    state_code varchar not null constraint dim_states_pkey primary key,
    state_name varchar
    
)
;
"""


create_dim_cities_demographics_summary = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_cities_demographics_summary
( 
    city varchar,
    median_age float,
    male_pop integer,
    female_pop integer,
    total_pop integer,
    num_vets integer,
    foreign_born integer,
    avg_household_size float,
    state_code varchar,
    PRIMARY KEY (state_code, city)
)
;
"""


create_dim_cities_demographics_race = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_cities_demographics_race
( 
    state_code varchar,
    city varchar,
    race varchar,
    count integer
)
;
"""

create_dim_ports = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_ports
(
    port_code varchar(3) primary key,
    port_city varchar,
    port_state varchar
)
;
"""


create_dim_arrival_modes = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_arrival_modes
(
    arrival_mode_code varchar primary key,
    arrival_mode_type char(12)
)

;
"""


create_dim_visa_intents_types = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_visa_intents_types
(
    visa_intents_code varchar primary key,
    visa_intents_type char(8)
)

;
"""


# create_dim_visa_types = f"""
# CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_visa_types
# (
#     visa_code varchar primary key,
#     visa_type varchar
# )

# ;
# """


copy_fact_immigration_events = f"""
COPY {staging_schema}.fact_immigration_events (cicid, i94yr, i94mon, i94cit, i94res, i94port,
arrdate, i94mode, i94addr, depdate, i94bir, i94visa,
count, dtadfile, visapost, occup, entdepa, entdepd,
entdepu, matflag, biryear, dtaddto, gender, insnum,
airline, admnum, fltno, visatype)

FROM '{processed_bucket}/fact_immigration_events'
IAM_ROLE '{redshift_iamrole}'
DELIMITER '|'
removequotes
emptyasnull
blanksasnull
NULL AS  '\\000'
IGNOREHEADER 1
maxerror 10
;
"""

copy_dim_countries = f"""
COPY {staging_schema}.dim_countries
FROM '{processed_bucket}/dim_countries'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','
NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_states = f"""
COPY {staging_schema}.dim_states
FROM '{processed_bucket}/dim_states'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_cities_demographics_summary = f"""
COPY {staging_schema}.dim_cities_demographics_summary (state_code,city,median_age,male_pop,female_pop,total_pop,num_vets,foreign_born,avg_household_size)
FROM '{processed_bucket}/dim_cities_demographics_summary'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_cities_demographics_race = f"""
COPY {staging_schema}.dim_cities_demographics_race
FROM '{processed_bucket}/dim_cities_demographics_race'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_ports = f"""
COPY {staging_schema}.dim_ports
FROM '{processed_bucket}/dim_ports'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_arrival_modes = f"""
COPY {staging_schema}.dim_arrival_modes
FROM '{processed_bucket}/dim_arrival_modes'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_visa_intents_types = f"""
COPY {staging_schema}.dim_visa_intents_types
FROM '{processed_bucket}/dim_visa_intents_types'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

copy_dim_visa_types = f"""
COPY {staging_schema}.dim_visa_types
FROM '{processed_bucket}/dim_visa_types'
IAM_ROLE '{redshift_iamrole}'
CSV
DELIMITER ','

NULL AS  '\\000'
IGNOREHEADER 1
;
"""

staging_drop_US_immigration_tables = [
    drop_fact_immigration_events,
    drop_dim_countries,
    drop_dim_states,
    drop_dim_cities_demographics_summary,
    drop_dim_cities_demographics_race,
    drop_dim_ports,
    drop_dim_arrival_modes,
    drop_dim_visa_intents_types,
    drop_dim_visa_types,
]
staging_create_US_immigration_tables = [
    create_dim_countries,
    create_dim_states,
    create_dim_cities_demographics_summary,
    create_dim_cities_demographics_race,
    create_dim_ports,
    create_dim_arrival_modes,
    create_dim_visa_intents_types,
    create_fact_immigration_events,
]
staging_copy_tables = [
    copy_dim_countries,
    copy_dim_states,
    copy_dim_cities_demographics_summary,
    copy_dim_cities_demographics_race,
    copy_dim_ports,
    copy_dim_arrival_modes,
    copy_dim_visa_intents_types,
    copy_fact_immigration_events,
]

