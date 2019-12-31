########################################################################################################################
#CLASS NAME: TransformData
#Description: Commmon module for transformation. Selects specified columns from the dataframe and returns a tranformed
#              spark dataframe.
#Input Parameters:
#      a. Spark dataframe
#      b. Columns to be selected
########################################################################################################################

class TransformData:
    def __init__(self,spark):
        self.spark=spark


    def select_columns(self,df,select_columns=[]):
        if not select_columns:
            print("All columns selected")
            return df
        else:
            return df.selectExpr(select_columns)
