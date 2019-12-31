######################################################################################################################
#Program Description: This the main driver program that runs the ETL and validation process based on the config file #
#Execution flow:                                                                                                     #
#  1. config.cfg file orchestrates the flow of ETL process.                                                          #
#  2. FLOW section gives the order of exeuction through EXECUTION_ORDER with first DIM tables and then FACT          #
#  3. TABLES section has 2 types of tables FACT and DIM                                                              #
#  4. Each table has a section in the config file containing the following parameters:                               #
#     a. MODULE - module has the class that performs the ETL                                                         #
#     b. CLASS -  class that does the ETL                                                                            #
#     c. SOURCE_PATH -  Path for source file for the table                                                           #
#     d. TARGET_PATH -  target file path                                                                             #
#     e. FILE_FORMAT -  Source file format                                                                           #
#     f. OUTPUT_FORMAT -  Target file format                                                                         #
#     g. DELIMITER -  Delimiter of the source file                                                                   #
#     h. SELECT_COLUMNS -  Columns to be selected from source file                                                   #
#     i. PARTITION_BY -  Columns to be partitioned by while writing output                                           #
#     j. REFERNTIAL_INTEGRITY-Referential integrity of columns mapped to corresponding Dim table(only for fact table)#
#   5. Main program iterates as per the flow declared in config file and dynamically call the respective classes     #
#   6. After iterating and completing ETL for all tables, validation module is called.                               #
######################################################################################################################
import os
from pyspark.sql import SparkSession
import configparser
import importlib
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
config_file = os.path.join(os.path.dirname(__file__), 'config.cfg')

"""
ETL function iterates and calls respective table's ETL module to perform ETL for each table based on the config file. 
Input parameter: configparser object

"""
def ETL(config):
    config.read(filenames=[config_file])
    executionFlow = config.get("FLOW", "EXECUTION_ORDER").split(',')

    for flow in executionFlow:
        tables = config.get("TABLES", flow).split(',')
        for table in tables:
            handlerObj = getattr(importlib.import_module(config.get(table, "MODULE")), config.get(table, "CLASS"))(
                config, spark)
            handlerObj.main()

"""
Validation function calls validation module to perform data quality check of all tables and referential integrity 
check referential integrity check for fact table
Input parameter: configparser object
"""
def Validation(config):
    config.read(filenames=[config_file])
    handlerObj = getattr(importlib.import_module(config.get("VALIDATION", "MODULE")),
                         config.get("VALIDATION", "CLASS"))(
        config, spark)
    handlerObj.main()

"""
Trigger for the python script
"""
if __name__ == '__main__':
    config = configparser.ConfigParser()
    print("Start Time {}".format(datetime.now()))
    ETL(config)
    Validation(config)
    print("End Time {}".format(datetime.now()))
