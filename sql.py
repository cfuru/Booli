import pyodbc
import pandas as pd
import sys

class SQL:
    def __init__(self, queryfile, queryFilePath):
        self.queryfile = queryfile
        self.queryFilePath = queryFilePath
        self.driver= '{SQL Server}'
        self.server = 'DESKTOP-F0MM68K'
        self.database = 'christopherFuru'

    def connect(self):
        cnxn = pyodbc.connect('DRIVER=' + self.driver + \
                            ';SERVER='+ self.server + \
                            ';DATABASE='+ self.database + \
                            ';Trusted_Connection=yes')
        cursor = cnxn.cursor()
        return cnxn, cursor

    def executeQueryFromFile(self, cnxn):
        sqlpath = self.queryFilePath + self.queryfile
        
        # Open the file and save in variable sqlFile
        try:
            with open(sqlpath, 'r', encoding = "utf-8") as f:
                sqlFile = f.read()
        except Exception as e:
            print("When trying to read file this error occured:  ", e)
            f.close()
            sys.exit(1)
        finally:
            f.close()

        # Try to run query or raise an error
        try:
            df = pd.read_sql_query(sqlFile, cnxn)
        except Exception as e:
            print("When trying to execute file this error occured:  ", e)
            sys.exit(1)
        return df
