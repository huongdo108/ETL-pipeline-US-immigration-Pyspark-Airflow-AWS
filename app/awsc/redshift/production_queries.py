import configparser
from pathlib import Path
import os

US_immigration_schema = os.environ["production_schema"]

create_production_schema = f"CREATE SCHEMA IF NOT EXISTS {US_immigration_schema};"

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
    arrival_mode_code varchar  primary key,
    arrival_mode_type char(12)
)

;
"""


create_dim_visa_intents_types = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_visa_intents_types
(
    visa_intents_code varchar  primary key,
    visa_intents_type char(8)
)

;
"""


create_dim_visa_types = f"""
CREATE TABLE IF NOT EXISTS {US_immigration_schema}.dim_visa_types
(
    visa_code varchar  primary key,
    visa_type char(8)
)

;
"""


production_drop_US_immigration_tables = [
    drop_fact_immigration_events,
    drop_dim_countries,
    drop_dim_states,
    drop_dim_cities_demographics_summary,
    drop_dim_cities_demographics_race,
    drop_dim_ports,
    drop_dim_arrival_modes,
    drop_dim_visa_intents_types,
]
production_create_US_immigration_tables = [
    create_dim_countries,
    create_dim_states,
    create_dim_cities_demographics_summary,
    create_dim_cities_demographics_race,
    create_dim_ports,
    create_dim_arrival_modes,
    create_dim_visa_intents_types,
    create_fact_immigration_events,
]

