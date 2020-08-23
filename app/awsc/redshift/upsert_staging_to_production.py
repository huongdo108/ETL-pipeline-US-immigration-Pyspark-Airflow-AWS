import configparser
from pathlib import Path
import os

staging_schema = os.environ["staging_schema"]
production_schema = os.environ["production_schema"]

upsert_fact_immigration_events = f"""
BEGIN TRANSACTION;

DELETE FROM {production_schema}.fact_immigration_events
using {staging_schema}.fact_immigration_events
where {production_schema}.fact_immigration_events.immigration_id = {staging_schema}.fact_immigration_events.immigration_id;

INSERT INTO {production_schema}.fact_immigration_events (cicid, i94yr, i94mon, i94cit, i94res, i94port,
arrdate, i94mode, i94addr, depdate, i94bir, i94visa,
count, dtadfile, visapost, occup, entdepa, entdepd,
entdepu, matflag, biryear, dtaddto, gender, insnum,
airline, admnum, fltno, visatype)

SELECT cicid, i94yr, i94mon, i94cit, i94res, i94port,
arrdate, i94mode, i94addr, depdate, i94bir, i94visa,
count, dtadfile, visapost, occup, entdepa, entdepd,
entdepu, matflag, biryear, dtaddto, gender, insnum,
airline, admnum, fltno, visatype FROM {staging_schema}.fact_immigration_events;

END TRANSACTION ;
COMMIT;
"""

upsert_dim_countries = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_countries
using {staging_schema}.dim_countries
where {production_schema}.dim_countries.country_code = {staging_schema}.dim_countries.country_code;
INSERT INTO {production_schema}.dim_countries
SELECT * FROM {staging_schema}.dim_countries;
END TRANSACTION ;
COMMIT;
"""


upsert_dim_states = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_states
using {staging_schema}.dim_states
where {production_schema}.dim_states.state_code = {staging_schema}.dim_states.state_code;
INSERT INTO {production_schema}.dim_states
SELECT * FROM {staging_schema}.dim_states;
END TRANSACTION ;
COMMIT;
"""

upsert_dim_cities_demographics_summary = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_cities_demographics_summary
using {staging_schema}.dim_cities_demographics_summary
where {production_schema}.dim_cities_demographics_summary.state_code = {staging_schema}.dim_cities_demographics_summary.state_code
and {production_schema}.dim_cities_demographics_summary.city = {staging_schema}.dim_cities_demographics_summary.city;
INSERT INTO {production_schema}.dim_cities_demographics_summary
SELECT * FROM {staging_schema}.dim_cities_demographics_summary;
END TRANSACTION ;
COMMIT;
"""

upsert_dim_cities_demographics_race = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_cities_demographics_race
using {staging_schema}.dim_cities_demographics_race
where {production_schema}.dim_cities_demographics_race.state_code = {staging_schema}.dim_cities_demographics_race.state_code
and {production_schema}.dim_cities_demographics_race.city = {staging_schema}.dim_cities_demographics_race.city;
INSERT INTO {production_schema}.dim_cities_demographics_race
SELECT * FROM {staging_schema}.dim_cities_demographics_race;
END TRANSACTION ;
COMMIT;
"""

upsert_dim_ports = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_ports
using {staging_schema}.dim_ports
where {production_schema}.dim_ports.port_code = {staging_schema}.dim_ports.port_code;
INSERT INTO {production_schema}.dim_ports
SELECT * FROM {staging_schema}.dim_ports;
END TRANSACTION ;
COMMIT;
"""

upsert_dim_arrival_modes = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_arrival_modes
using {staging_schema}.dim_arrival_modes
where {production_schema}.dim_arrival_modes.arrival_mode_code = {staging_schema}.dim_arrival_modes.arrival_mode_code;
INSERT INTO {production_schema}.dim_arrival_modes
SELECT * FROM {staging_schema}.dim_arrival_modes;
END TRANSACTION ;
COMMIT;
"""

upsert_dim_visa_intents_types = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_visa_intents_types
using {staging_schema}.dim_visa_intents_types
where {production_schema}.dim_visa_intents_types.visa_intents_code = {staging_schema}.dim_visa_intents_types.visa_intents_code;
INSERT INTO {production_schema}.dim_visa_intents_types
SELECT * FROM {staging_schema}.dim_visa_intents_types;
END TRANSACTION ;
COMMIT;
"""

upsert_dim_visa_types = f"""
BEGIN TRANSACTION;
DELETE FROM {production_schema}.dim_visa_types
using {staging_schema}.dim_visa_types
where {production_schema}.dim_visa_types.visa_code = {staging_schema}.dim_visa_types.visa_code;
INSERT INTO {production_schema}.dim_visa_types
SELECT * FROM {staging_schema}.dim_visa_types;
END TRANSACTION ;
COMMIT;
"""

upsert_staging_to_production_queries = [
    upsert_fact_immigration_events,
    upsert_dim_countries,
    upsert_dim_states,
    upsert_dim_cities_demographics_summary,
    upsert_dim_cities_demographics_race,
    upsert_dim_ports,
    upsert_dim_arrival_modes,
    upsert_dim_visa_intents_types,
]
