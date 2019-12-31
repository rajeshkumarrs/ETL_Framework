########################################################################################################################
#CLASS NAME: States
#Description:
# Performs ETL for US_States dimension.
#    1. Gets the spark session and configparser object from the main script
#    2. Creates objects of the CommonHandler module classes for extraction, tranformation, cleansing and load
#    3. Adds any additional transformation or cleansing on top of CommonHandler module
########################################################################################################################

from CommonHandlers.extraction import ExtractData
from CommonHandlers.data_cleansing import DataCleansing
from CommonHandlers.transformation import TransformData
from CommonHandlers.data_load import DataLoad

from pyspark.sql.functions import col
from pyspark.sql.functions import *

table_name = "STATES"


class States:
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
        # df = self.spark.read.format("csv").option("header", "true").option("delimiter", delimiter).option(
        #     "inferschema", "true").option("quote","'").load(path)
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
        cleansedDF = df
        distinctDF = self.commonHandlerObj_cleansing.data_cleansing(cleansedDF)
        print("Complete data cleansing for {} dim".format(table_name))
        return distinctDF

    def load_data(self, df):
        print("Data Load for {} begin".format(table_name))
        path = self.config.get(table_name, "TARGET_PATH")
        output_format = self.config.get(table_name, "OUTPUT_FORMAT")
        self.commonHandlerObj_dataload.loadFile(df=df, path=path, file_format=output_format)
        print("Data Load complete")

    def main(self):
        extractedDF = self.extract()
        extractedDF.show(5, False)

        transformedDF = self.transformation(extractedDF)
        transformedDF.show(5,False)

        cleansedDF = self.data_cleansing(transformedDF)
        cleansedDF.show(5,False)

        self.load_data(cleansedDF)
