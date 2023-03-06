# import library
import pandas as pd
import cx_Oracle
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import os, shutil, zipfile
from shutil import rmtree
import datetime as dt
import sys
import textwrap

class extractDatabaseData:
    def __init__(self):
        self.engine     = sqlalchemy.create_engine("oracle+cx_oracle://SIE_TI_BD:Cloud2019@192.168.0.30:1521/?service_name=SIESA", arraysize=9000)
        self.conn       = self.engine.connect()
        self.outputDir  = f"/home/pydev/workflow/dt_customer_portfolio/data/"
        self.inSqlFiles = f"/home/pydev/workflow/dt_customer_portfolio/sql/queryPortfolio.sql"
        
    def extractData(self, name):
        with open(self.inSqlFiles, 'r') as self.f_in:
            self.lines = self.f_in.read()
            self.query_string = textwrap.dedent("""{}""".format(self.lines))
            df_sqlfact = pd.read_sql(self.query_string, self.conn)
            df_sqlfact.to_csv(f"{self.outputDir}{name}", index=None, header=False, sep='\t', encoding='utf-8', lineterminator='\n')

data = extractDatabaseData()
data.extractData("customerPortfolio.txt")