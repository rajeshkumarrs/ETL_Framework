########################################################################################################################
#CLASS NAME: ExtractData
#Description: Commmon module for extraction. Reads the file as the file format and returns spark dataframe.
#Input Parameters:
#      a. Source file path
#      b. file format - text, parquet etc
#      c. delimiter
########################################################################################################################

class ExtractData:
    def __init__(self, spark):
        self.delimiter = ""
        self.path = ""
        self.spark = spark

    def extractText(self):
        df = self.spark.read.format("csv").option("header", "true").option("delimiter", self.delimiter).option(
            "inferschema", "true").load(self.path)
        return df

    def extractParquet(self):
        df=self.spark.read.parquet(self.path)
        return df

    def extractFile(self, path, file_format, delimiter=""):

        print("inside common extraction handler")
        self.delimiter = delimiter
        self.path = path

        if (file_format== "text"):
            return self.extractText()
        elif(file_format=="parquet"):
            return self.extractParquet()
