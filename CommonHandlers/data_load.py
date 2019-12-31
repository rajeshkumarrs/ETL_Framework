########################################################################################################################
#CLASS NAME: DataLoad
#Description: Commmon module for loading the target. Currently all tables are created as parquet file
#Input Parameters:
#      a. Spark dataframe
#      b. output file format - eg: parquet
#      c. delimiter - delimiter in case of creating a text file target
#      d. partition_by - in case the output file needs to be partitioned.
########################################################################################################################

class DataLoad:
    def __init__(self,spark):
        self.spark=spark
        self.delimiter=""
        self.path=""


    def writeParquest(self,df,parition_by):
        if not parition_by:
            df.write.mode('overwrite').parquet(self.path)
        else:
            df.write.partitionBy(parition_by).mode('overwrite').parquet(self.path)

    def loadFile(self, df, path, file_format, delimiter="",partition_by=[]):
        self.delimiter = delimiter
        self.path = path

        if (file_format== "parquet"):
            return self.writeParquest(df,partition_by)
