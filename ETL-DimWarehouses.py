import pandas as pd
import urllib
from sqlalchemy import create_engine
#
#Connection string for sqlalchemy
connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-P3CJ1SG\\SQLEXPRESS;Database=MyStoreDW;Trusted_Connection=yes;"
#connection_string ="Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=MYStoreDW;Trusted_Connection=yes;"
connection_string = urllib.parse.quote_plus(connection_string)
connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
#
# Create a sqlalchemy engine
engine = create_engine(connection_string)
#
def csv_to_tables(fileName,tableName,conStr):
    engine = create_engine(conStr)
    df = pd.read_csv(fileName)
    df.to_sql(name=tableName, con=engine, if_exists='append', index=False)
    engine.dispose()
#    
# Load the tables from CSV files
csv_to_tables('Warehouse_Info.csv','DimWarehouses',connection_string)
#
# Close the engine
engine.dispose()
#


