# US Immigration Events ETL pipeline

## Overview
This project demonstrates a data engineering ETL pipeline from ingesting data from source to modelling data in warehouse.

<img src="https://github.com/huongdo108/ETL-pipeline-US-immigration-Pyspark-Airflow-AWS/blob/master/images/ETL.PNG" align="centre">

## Project Guideline
* Step 1: Scope the Project and Gather data
* Step 2: Explore and Assess the data
* Step 3: Define the Data Model 
* Step 4: Run ETL process to ingest, transform, and load data to datawarehouse
* Step 5: Run quality check on data in datawarehouse
* Step 6: Run analytics on data in datawarehouse
* Step 5: Addressing Other Scenarios

### Scope 
The scope of the this project is to build an ETL process for US immigration events. Data is gathered, assessed, cleaned, and built into star chema data models stored in data warehouse for data analytics tasks.

### Data
Data is provided by Udacity.

- **i94 SAS datasets 2016** The data contains 12 sas files of US immigration events in 2016 (6 GB).
- **US cities demographics** The data contains demographics information of US cities.
- **i94 SAS Labels Descriptions**. The data is processed into below datasets:
    - countries_codes.txt
    - modes.txt
    - ports.txt
    - us_states.txt
    - visa_intents_types.txt

### Workflow

#### 1. ETL pipeline
- Raw data is moved from the source to S3 input bucket.
- ETL job copies raw data from S3 input bucket to processing bucket.
- Spark job is triggered to read the data from processing bucket and process the data on AWS EMR clusters. In this step, data is cleaned, transformed, and finally moved to processed bucket. 
- ETL job picks up data from processed bucket and stages it into AWS Redshift staging tables.
- ETL job performs UPSERT operation to upload and insert data from staging tables into production tables in Redshift.
- ETL pipeline execution is completed.

#### 2. Data quality check
- Once the ELT pipeline execution is completed, DAG runs data quality check on all production tables in Redshift.
- Data quality check include: check null values, and existing values.
- DAG job execution is completed.

#### 3. Data analytics 
- Once data quality check jobs are completeted, DAG run data analytics queries to perform some ad hoc analysis: which countries that have highest amount of people immigrate to US, which cities that are mostly visited by immigrants, what is the most reason to visit the US by country.

### Tools and technologies used and setup

#### 1. Tools and technologies
- Python : The programming language used in the project.
- Pandas : Python data library used in initial data exploration and cleaning.
- PySpark : Pyspark is used to process large immigration data in batch on aws emr.
- AWS S3 : Amazon Simple Storage Service used to store raw data before processing.
- AWS EMR: Amazon cluster used to process data with Spark.
- AWS Redshift : Amazon Redshift used as warehousing database to perform query analysis.
- Docker: to setup Airflow on Window
- Airflow: to run ETL, data quality check, and data analytics jobs.


#### 2. Set up Airflow with Docker on Window
- Prepare Dockerfile used base image puckel/docker-airflow
- Build Docker image
```powershell
docker build . --tag <image_name>:<image_tag>
```
- Run Docker container and match volumne to airflow dags, airflow plugins, and app folder. (I have to match volume separately to different folders because it did not work properly if I match to only 1 wrapping folder)
```powershell
docker run --detach --publish 8080:8080 -v /path/to/airflow/dags/on/your/local/machine/:/usr/local/airflow/dags/ -v /path/to/airflow/plugins/on/your/local/machine/:/usr/local/airflow/plugins/ -v /path/to/app/on/your/local/machine/:/home/workspace/ --name <container_name> <image_name>:<image_tag> webserver

```
**Docker useful commands**

- Get list of running containers
```powershell
docker ps
```

- Get list of all containers
```powershell
docker ps -a
```

- Get into the container with command 
```powershell
docker exec -it <container_name> bash
``` 
- Stop container
```powershell
docker stop <container_name>
```
- Force stop and remove container
```powershell
docker rm -f <container_name>
```

#### 3. Set up AWS services: S3, EMR, Redshift
There are 2 ways to create and set up credentials for aws services: via AWS console or use IaS with boto3.

**S3**

Upload bootstrap.sh file to S3 input bucket.

**EC2**

Create an SSH keypair on EC2. Download the file and replace the spark-cluster.pem file in app folder.

**EMR**

Add bootstrap action (use the bootstrap file uploaded to S3 input bucket) to install neccessary packages and their dependencies.

Add EC2 keypair (created previously at EC2 step) to SSH into master node of the EMR cluster.

### How to run

- Change the config.cfg file with your AWS credentials.

- Open Airflow UI at http://localhost:8080/admin/.  

- Add connections to EMR and Redshift using your own AWS credentials.

- Trigger dag

**Completed dag**

Here is how it looks after airflow completes all the tasks.

<img src="https://github.com/huongdo108/ETL-pipeline-US-immigration-Pyspark-Airflow-AWS/blob/master/images/DAG2.PNG" align="centre">    


<img src="https://github.com/huongdo108/ETL-pipeline-US-immigration-Pyspark-Airflow-AWS/blob/master/images/DAGtree.PNG" align="centre">  


### Conceptual data model and data dictionary

#### 1. Conceptual data model
Data Model is built with Star Schema. 

Fact table consists of US immigration events happened in 2016. Dimension tables consist of the data that are related to dimensions in the fact table: countries, states, cities demographics, visa intention types, arrival mode types, ports.  

#### 2. Data dictionary

**Fact Table** - Immigration data Events: fact_immigration_events

- immigration_id = primary key
- cicid = unique key within a month
- i94yr = 4 digit year,
- i94mon = numeric month,
- i94cit = 3 digit code of immigrant's country of citizenship,
- i94res = immigrant's country of residence outside US,
- i94port = port of entry, 3 character code of destination USA city,
- arrdate = arrival date in the USA,
- i94addr = address in the USA,
- i94mode = mode of arrival, 1 digit travel code,
- depdate = departure date from the USA,
- i94bir = age,
- i94visa = visa,
- count = used for summary statistics; always 1 
- dtadfile = dates in the format YYYYMMDD,
- visapost = three-letter codes corresponding to where visa was issued,
- occup = occupation,
- entdepa = one-letter arrival code,
- entdepd = one-letter departure code,
- entdepu = one-letter update code,
- matflag = M if the arrival and departure records match,
- biryear = birth year,
- dtaddto = MMDDYYYY date field for when the immigrant is admitted until,
- gender = gender,
- insnum = Immigration and Naturalization Services number; many re-used,
- airline = Airline of entry for immigrant,
- admnum = admission number; many re-used, but not as much as insnum,
- fltno = flight number of immigrant,
- visatype = reason for immigration (short visa codes like WT, B2, WB, etc.),

**Dimension Table** - Countries: dim_countries

- country_code = country code
- country_name = country name

**Dimension Table** - States: dim_states

- state_code = State Code,
- state_name = state

**Dimension Table** - Cities: dim_cities_demographics_summary

- city = 3 character code of destination city (mapped from cleaned up immigration data),
- median_age = median age,
- male_pop = male population,
- female_pop = female population,
- total_pop = Total Population,
- num_vets = Number of Veterans,
- foreign_born = Foreign-born,
- avg_household_size = Average Household Size,
- state_code = State Code, can be null,
- PRIMARY KEY (state_code, city)

**Dimension Table** - Cities: dim_cities_demographics_race

- state_code = State Code, can be null,
- city = 3 character code of destination city (mapped from cleaned up immigration data),
- race = White, Hispanic or Latino, Asian, Black or African-American, or American Indian and Alaska Native,
- count = number of people of that race in the city

**Dimension port** A list of the ports of arrival: dim_ports

- port_code = a short code
- port_city = the name of the city where the port is in
- port_state = the name of the state where the port is in

**Dimension arrival mode** How immigrants arrived. Foreign key to fact_immigration.i94mode: dim_arrival_modes

- arrival_mode_code = 1, 2, 3, or 9
- arrival_mode_type = Air, Sea, Land, or Not reported, respectively

**Dimension visa intents type** The type of visa the immigrant is coming in on. Foreigy key to fact_immigration.i94visa: dim_visa_intents_types

- visa_intents_code = 1, 2, or 3
- visa_intents_type = Business, Pleasure, or Student, respectively


### Addressing Other Scenarios
#### How often the data should be updated.
- Fact table should be updated everyday as the immigration events happen everyday.
- Dimension tables are more static and can be updated weekly or monthly.

#### How to approach the problem differently under the following scenarios:
- **The data was increased by 100x:** Store data in s3 or Hadoop distributed file system. Process data with EMR and Spark with more or stronger cluster nodes. Store data warehouse in Redshift which is optimized for data aggregation and can handle read-heavy workload.

- **The data populates a dashboard that must be updated on a daily basis by 7am every day.**  Airflow can be sheduled to run jobs on daily basis before 7 am. Airflow DAG can sends email alert if a task fails.

- **The database needed to be accessed by 100+ people.** Redshift can support upvto 500 connections. And we can launch as many clusters as the traffic demands.
