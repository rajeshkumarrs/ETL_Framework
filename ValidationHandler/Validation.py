########################################################################################################################
# CLASS NAME: Validation
# Description: Validation class check the data quality of the dimensions and facts created. It does the following data
#             quality check
#             1. If all targets have the proper record counts
#             2. Checks for referential integrity between fact table and dimension tables.
########################################################################################################################

from pyspark.sql.functions import col


class Validation:
    def __init__(self, config, spark):
        self.spark = spark
        self.config = config

    def getRecordCount(self, path):
        return self.spark.read.parquet(path).count()

    def checkRefIntegrity(self, table):
        path = self.config.get(table, "TARGET_PATH")
        factDF = self.spark.read.parquet(path)
        refInt = self.config.get(table, "REFERNTIAL_INTEGRITY").split(',')
        for ri in refInt:
            for factColumn, reference in eval(ri).items():
                for dimTable, column in reference.items():
                    dimPath = self.config.get(dimTable, "TARGET_PATH")
                    dimDF = self.spark.read.parquet(dimPath).select(col(column)).distinct()
                    print("Verifying all {} column values from fact are present in {} dim table".format(factColumn,
                                                                                                        dimTable))
                    if(factDF.select(col(factColumn)).distinct().join(dimDF,factDF[factColumn] == dimDF[column], "leftanti").count()>0):
                        print("{} column has values not present in {} column of {} dim table".format(factColumn,column,dimTable))
                    else:
                        print("All {} column values of fact table are present in {} dim table".format(factColumn,dimTable))

    def main(self):
        execitonFlow = self.config.get("FLOW", "EXECUTION_ORDER").split(',')
        print("Validating successful load for all tables - Begin")
        for flow in execitonFlow:
            tables = self.config.get("TABLES",flow).split(',')
            for table in tables:
                output_file = self.config.get(table,"TARGET_PATH")
                print("Validation for {} table".format(table))
                record_count=self.getRecordCount(output_file)
                print(record_count)
                if(record_count>0):
                    print("Data loaded successfully to {} table".format(table))
        print("Validating successful load for all tables - Complete")
        print("Validating referential integrity of fact table - Begin")
        factTables = self.config.get("TABLES", "FACT").split(',')
        for factTable in factTables:
            self.checkRefIntegrity(factTable)
        print("Validating referential integrity of fact table - Complete")
