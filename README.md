# # ETL Framework - US IMMIGRATION DATA ANALYTICS

## Scope of the project 
Project is to gather all immigration data of US along with dimensions like demogprahics, airports, states, temperatures, and create a dataware house with facts and dimensions. This project will be carried out using Spark and, fact and dimension tables are created as parquet files. 

## Describe source data
##### **I94 Immigration Data**: 
This data comes from the US National Tourism and Trade Office.This data comes from the US National Tourism and Trade Office. 
##### **World Temperature Data**: 
Global temperature data by City. This dataset came from Kaggle
##### **airport-codes**: 
This is a simple table of airport codes and corresponding cities. This data comes from datahub.io
##### **airports**: 
Contains City and State of US airport codes. This data is taken from I94_SAS_Labels_Description.SAS
##### **countries**: 
Country code and country names of all countries in the I94 immigration data. This data is taken from I94_SAS_Labels_Description.SAS
##### **modes**: 
Different types of travel modes. This data is taken from I94_SAS_Labels_Description.SAS
##### **U.S. City Demographic Data**: 
comes from OpenSoft and includes data by city, state, age, population, veteran status and race.
##### **US states**: 
Contains all states in US. This data is taken from I94_SAS_Labels_Description.SAS
##### **visa**: 
Contains different visa types. This data is taken from I94_SAS_Labels_Description.SAS

## Explore and assess data
### Common cleansing
1. Remove leading and trailing spaces from all string columns
2. Remove any duplicates

### Table specific cleansing
**immigration**: Rename columns to understandable name, Create arrival date and departure date columns, filter records that do not have valid state and country code. 
**temperature**: Filter only United States temperature data. 
**airport_codes**: Filter only United states airport codes. Type cast number fields to integer or float as appropriate. 

## Data Model
### Fact: immigration
**immigration**
 |-- cic_id: integer (nullable = true)
 |-- arrival_date: date (nullable = true)
 |-- visa_category: integer (nullable = true)
 |-- visatype: string (nullable = true)
 |-- origin_country: integer (nullable = true)
 |-- cit_country: integer (nullable = true)
 |-- state: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- birth_year: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- departure_date: date (nullable = true)
 |-- port_of_entry: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 
 ## Dimensions
 **airport_codes**
 |-- ident: string (nullable = true)
 |-- type: string (nullable = true)
 |-- name: string (nullable = true)
 |-- elevation_ft: float (nullable = true)
 |-- iso_country: string (nullable = true)
 |-- iso_region: string (nullable = true)
 |-- municipality: string (nullable = true)
 
 **airports**
 |-- code: string (nullable = true)
 |-- city_name: string (nullable = true)

**countries**
 |-- country_code: integer (nullable = true)
 |-- country_name: string (nullable = true)

**us_demographics**
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- Median_Age: double (nullable = true)
 |-- Male_Population: integer (nullable = true)
 |-- Female_Population: integer (nullable = true)
 |-- Total_Population: integer (nullable = true)
 |-- Number_of_Veterans: integer (nullable = true)
 |-- Foreign_born: integer (nullable = true)
 |-- Average_Household_Size: double (nullable = true)
 |-- State_Code: string (nullable = true)
 |-- Race: string (nullable = true)
 |-- Count: integer (nullable = true)
 
 **modes**
 |-- code: integer (nullable = true)
 |-- mode_name: string (nullable = true)
 
 **US_States**
 |-- state_code: string (nullable = true)
 |-- state_name: string (nullable = true)
 
 
 **GlobalLandTemperaturesByCity**
 |-- dt: timestamp (nullable = true)
 |-- AverageTemperature: double (nullable = true)
 |-- AverageTemperatureUncertainty: double (nullable = true)
 |-- City: string (nullable = true)
 |-- Country: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Longitude: string (nullable = true)


**visa**
 |-- code: integer (nullable = true)
 |-- visa: string (nullable = true)
 
 ## ETL design

ETL is design as a framework in which common ETL functionalities across all targets are in it and then individual table based handlers to handle the table specific ETL processes. The entire flow is made to orchestrate from a config file. If any new dimensions or facts tables to be added in the future, it will be only a delta effort to create their new respective table handlers,  updating the config file with those details and including those tables in the execution flow. 

### **Execution flow**:                                                                                                     
  1. config.cfg file orchestrates the flow of ETL process.                                                          
  2. FLOW section gives the order of exeuction through EXECUTION_ORDER with first DIM tables and then FACT          
  3. TABLES section has 2 types of tables FACT and DIM                                                              
  4. Each table has a section in the config file containing the following parameters:                               
     a. MODULE - module has the class that performs the ETL                                                         
     b. CLASS -  class that does the ETL                                                                            
     c. SOURCE_PATH -  Path for source file for the table                                                           
     d. TARGET_PATH -  target file path                                                                             
     e. FILE_FORMAT -  Source file format                                                                           
     f. OUTPUT_FORMAT -  Target file format                                                                         
     g. DELIMITER -  Delimiter of the source file                                                                   
     h. SELECT_COLUMNS -  Columns to be selected from source file                                                   
     i. PARTITION_BY -  Columns to be partitioned by while writing output                                           
     j. REFERNTIAL_INTEGRITY-Referential integrity of columns mapped to corresponding Dim table(only for fact table)
   5. Main program iterates as per the flow declared in config file and dynamically call the respective classes     
   6. After iterating and completing ETL for all tables, validation module is called.                               

## Conclusion

**Rationale for the tool choice**: Since the data volume is huge, spark became the obvious choice for data processing. Spark being massively parallel processing through distributed cache and in-memory processing capability it will be able to work efficiently at scale. Output files are written as parquet which also gives a good data compression and easy retrieval of data through Spark. Parquet is the default file format of spark. 

**Proposed data update**: My proposal would be to load fact periodically (say every 4 hours) and dimensions once a day. Loading facts more than once a day will have relatively lesser volume of data during the batch run. 

**data increases by 100x**: Since we are using spark, spark will be able to handle the volume shootup. However, spark cluster need to be configured appropriately with sufficient number of master and worker nodes. Latest technologies like Databricks provides pre-configured cluster configuration and auto-scaling capability which can be leveraged for such volume shootups. 

**Dashboard data refresh**: Separate ETL process need to be developed to refresh the data mart for dashboard. We could use Airflow to schedule the ETL job that can be triggered at 7am every day. 

**Data to be accessed by 100+ concurrent users**: We can create templated notebooks with Spark SQLs which can be used to write sqls on the data warehouse. Spark will be able to handle concurrent user data access. In case more spohisticated data layer is needed, then Hive could be created as external table for the parquet files which can as well handle concurrent users. 
