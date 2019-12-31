########################################################################################################################
#CLASS NAME: Immigration
#Description:
# Performs ETL for immigration fact.
#    1. Gets the spark session and configparser object from the main script
#    2. Creates objects of the CommonHandler module classes for extraction, tranformation, cleansing and load
#    3. Adds any additional transformation or cleansing on top of CommonHandler module
########################################################################################################################

from CommonHandlers.extraction import ExtractData
from CommonHandlers.data_cleansing import DataCleansing
from CommonHandlers.transformation import TransformData
from CommonHandlers.data_load import DataLoad

from pyspark.sql.functions import col, lit
from pyspark.sql.functions import *

table_name = "IMMIGRATION"


class Immigration:
    def __init__(self, config, spark):
        print("starting ETL for {}".format(table_name))
        self.config = config
        self.spark = spark
        self.commonHandlerObj_extract = ExtractData(spark)
        self.commHandlerObj_transform = TransformData(spark)
        self.commonHandlerObj_cleansing = DataCleansing(spark)
        self.commonHandlerObj_dataload = DataLoad(spark)

    def extract(self):
        print("Begin extraction for {} dim".format(table_name))
        path = self.config.get(table_name, "SOURCE_PATH")
        file_format = self.config.get(table_name, "FILE_FORMAT")
        delimiter = self.config.get(table_name, "DELIMITER")
        df = self.commonHandlerObj_extract.extractFile(path, file_format, delimiter)
        print("extraction complete for {} dim".format(table_name))
        return df

    def transformation(self, df):
        print("Begin transformation for {} dim".format(table_name))
        select_columns = self.config.get(table_name, "SELECT_COLUMNS").split(',')
        df = self.commHandlerObj_transform.select_columns(df, select_columns)
        print("transformation complete for {} dim".format(table_name))
        return df

    def data_cleansing(self, df):
        print("Begin data cleansing for {} dim".format(table_name))
        cleansedDF = df.withColumn("cic_id", col("cic_id").cast("integer")).withColumn("year", col("year").cast(
            "integer")).withColumn("month", col("month").cast(
            "integer")).withColumn("visa_category", col("visa_category").cast("integer")).withColumn("origin_country",
                                                                                                     col(
                                                                                                         "origin_country").cast(
                                                                                                         "integer")).withColumn(
            "cit_country", col("cit_country").cast("integer")).withColumn(
            "sas_base_date", to_date(lit("01/01/1960"), "MM/dd/yyyy")).withColumn("arrival_date",
                                                                                  expr(
                                                                                      "date_add(sas_base_date,arrdate)")).withColumn(
            "departure_date", expr("date_add(sas_base_date,depdate)")).withColumn("age", col("age").cast(
            "integer")).withColumn("birth_year", col("birth_year").cast("integer")).drop("sas_base_date", "arrdate",
                                                                                         "depdate")
        statePath = self.config.get("STATES","TARGET_PATH")
        stateDF=self.spark.read.parquet(statePath)
        cleansedDF=cleansedDF.join(stateDF,cleansedDF.state == stateDF.state_code,"leftsemi")
        countryPath = self.config.get("COUNTRIES","TARGET_PATH")
        countryDF = self.spark.read.parquet(countryPath)
        cleansedDF = cleansedDF.join(countryDF, cleansedDF.origin_country == countryDF.country_code, "leftsemi")
        cleansedDF = cleansedDF.join(countryDF, cleansedDF.cit_country == countryDF.country_code, "leftsemi")
        distinctDF = self.commonHandlerObj_cleansing.data_cleansing(cleansedDF)
        print("Complete data cleansing for {} dim".format(table_name))
        return distinctDF

    def load_data(self, df):
        print("Data Load for {} begin".format(table_name))
        path = self.config.get(table_name, "TARGET_PATH")
        output_format = self.config.get(table_name, "OUTPUT_FORMAT")
        partition_by = self.config.get(table_name, "PARTITION_BY").split(',')
        self.commonHandlerObj_dataload.loadFile(df=df, path=path, file_format=output_format, partition_by=partition_by)
        print("Data Load complete")

    def main(self):
        extractedDF = self.extract()
        extractedDF.show(5)

        transformedDF = self.transformation(extractedDF)
        transformedDF.show(5)

        cleansedDF = self.data_cleansing(transformedDF)
        cleansedDF.show(5)

        self.load_data(cleansedDF)
