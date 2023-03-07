# Import librarys python
import pandas as pd
import glob
from datetime import datetime
import numpy as np
from pyspark import StorageLevel
from pyspark.sql.types import StructField, DecimalType, StringType, IntegerType, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql import functions as sf, Window as w
from pyspark.sql.functions import *
from pyspark.sql import SparkSession


class transformCustomerPortfolio:
    def __init__(self):
        # self.schema = StructType() \
        #     .add('rowidDoc', IntegerType(), True) \
        #     .add('codCia', IntegerType(), True) \
        #     .add('fechaDoc', DateType(), True) \
        #     .add('rowidTercero', StringType(), True) \
        #     .add('nit', StringType(), True) \
        #     .add('razonSocial', StringType(), True) \
        #     .add('codSucursal', StringType(), True) \
        #     .add('codCoDoc', StringType(), True) \
        #     .add('codCoMov', StringType(), True) \
        #     .add('codDoc', StringType(), True) \
        #     .add('consDoc', StringType(), True) \
        #     .add('lineaProv', StringType(), True) \
        #     .add('grupoShopper', StringType(), True) \
        #     .add('indNat', StringType(), True) \
        #     .add('vlrDeb', DecimalType(), True) \
        #     .add('vlrCred', DecimalType(), True)\
        #     .add('VlrSald', DecimalType(), True)\
        #     .add('vlrFact', DecimalType(), True)
            
        self.spark           = SparkSession.builder.master("local[*]").appName("customerPortfolio").getOrCreate()
        self.input_directory = '/home/pydev/workflow/dt_customer_portfolio/data/customerPortfolio.csv'

    def readFile(self):
        self.df_Data = self.spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", ",").load(self.input_directory)
        self.df_Data.show()    


data = transformCustomerPortfolio()
data.readFile()