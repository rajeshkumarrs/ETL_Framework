########################################################################################################################
#CLASS NAME: DataCleansing
#Description: Commmon module for data cleaning. Gets a spark dataframe as input and returns spark dataframe after
#             performing following cleansing
#               1. Remove leading and training spaces from string columns
#               2. Removes duplicates from the dataframe
#Input Parameters:
#      a. Spark dataframe
########################################################################################################################

from pyspark.sql.functions import col
from pyspark.sql.functions import *

class DataCleansing:
    def __init__(self,spark):
        self.spark=spark

    def getDataType(self,df,colname):
        return [dtype for name, dtype in df.dtypes if name == colname][0]

    def removeLeadingTrailingSpace(self,df):
        for colname in df.schema.names:
            if(self.getDataType(df,colname) == "string"):
                df.withColumn(colname,ltrim(rtrim(col(colname))))
        return df

    def data_cleansing(self,df):
        df = self.removeLeadingTrailingSpace(df)
        return df.dropDuplicates()