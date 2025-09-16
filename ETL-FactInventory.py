import pandas as pd
import json
import os
import pyodbc
import urllib
import datetime
import openpyxl
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING = True
#
# Define the connection strings to the MyStore and MyStoreDWETL databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-P3CJ1SG\\SQLEXPRESS;Database=MyStore;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_db
conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-P3CJ1SG\\SQLEXPRESS;Database=MyStoreDW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_dw
#####################################################################################
def create_Date_Key(inDate: datetime):
    in_Year = inDate.year
    in_Month = inDate.strftime("%m")
    in_DayNumber = inDate.strftime("%d")
    out_Date_Key = int (str(in_Year) + str(in_Month) + str(in_DayNumber))
    return out_Date_Key
#####################################################################################
def addNew_Date(chkDate: datetime, conStrdw):
    engine_dw = create_engine(conStrdw)
    sqlQuery = f'SELECT * FROM DimDate'
    dFrame = pd.read_sql_query(sqlQuery, engine_dw)
#   print(dFrame)
#
    Base = declarative_base()
    class DimDate(Base):
        __tablename__ = 'DimDate'
        Date_Key = Column(Integer, primary_key=True, autoincrement=False)
        Date = Column(DateTime)
        Year  = Column(Integer)
        Month  = Column(Integer)
        MonthName = Column(String)
        Week  = Column(Integer)
        WeekDay  = Column(Integer)
        WeekDayName = Column(String)
        DayNumber = Column(Integer)
#
    chk_Date = chkDate.strftime("%Y-%m-%d")
    chk_Year = chkDate.year
    chk_Month = chkDate.strftime("%m")
    chk_MonthName =chkDate.strftime('%B')
    chk_Week = int(chkDate.strftime('%U')) + 1
    chk_WeekDay = chkDate.weekday()+2
    chk_WeekDayName = chkDate.strftime('%A')
    chk_DayNumber = chkDate.strftime("%d")
    chk_Date_Key = int (str(chk_Year) + str(chk_Month) + str(chk_DayNumber))
    print(chk_Date_Key, chk_Date, chk_Year, chk_Month, chk_MonthName, chk_Week, chk_WeekDay, chk_WeekDayName, chk_DayNumber)
#
# Create a session
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
#
    if not session.query(DimDate).filter_by(Date_Key=chk_Date_Key).first():
        # If primary key does not exist, create a new object and add it to the session
          new_record = DimDate(Date_Key=chk_Date_Key, Date = chk_Date, Year = chk_Year, Month = chk_Month, MonthName = chk_MonthName,
                              Week=chk_Week, WeekDay=chk_WeekDay, WeekDayName=chk_WeekDayName, DayNumber = chk_DayNumber)
          session.add(new_record)
        # Commit the session to persist the changes to the database
          session.commit()
          print("Date rows successfully inserted into DimDate table.")
        # Close the session
          session.close()
    else:
          print(f"Skipping duplicate primary key:", chk_Date_Key)

#####################################################################################
def pipe_Inventory(whfile, conStrdw):
    engine_dw = create_engine(conStrdw)
    sqlQuery = f'select * from DimProducts'
    dFrame = pd.read_sql_query(sqlQuery, engine_dw)
#    Must pip install openpyxl
    _, file_extension = os.path.splitext(whfile)
    if file_extension.lower() == '.xlsx' or file_extension.lower() == '.xls':
        dFrameWH= pd.read_excel(whfile, sheet_name='Warehouse1')
    elif file_extension.lower() == '.json':
        data = json.load(open(whfile))
        dFrameWH = pd.DataFrame(data)
    dFrameWH['Stock_Date'] = pd.to_datetime(dFrameWH['Stock_Date'])
#    engine_dw = create_engine(conStrdw)
    Base = declarative_base()
    class FactInventory(Base):
        __tablename__ = 'FactInventory'
        WTransID = Column(Integer, primary_key=True, autoincrement=True)
        Warehouse_ID = Column(Integer)
        product_id = Column(Integer)
        Stock_Date = Column(DateTime)
        Stock_Quantity = Column(Integer)
        Stock_Date_Key = Column(Integer)
#
    class DimDate(Base):
        __tablename__ = 'DimDate'
        Date_Key = Column(Integer, primary_key=True, autoincrement=False)
        Date = Column(DateTime)
        Year  = Column(Integer)
        Month  = Column(Integer)
        MonthName = Column(String)
        Week  = Column(Integer)
        WeekDay  = Column(Integer)
        WeekDayName = Column(String)
        DayNumber = Column(Integer)
#
# Clean the dataframe dFrameWH
# Remove duplicate rows from dFrameWH with the same product_id, Stock_Date combination
    drop_record_str = ''
    dFrameWH = dFrameWH.drop_duplicates(subset=['product_id', 'Stock_Date'])
# Remove rows from dFrameWH with non-numeric Stock Quantity values
    rows_to_drop = []
    for index, row in dFrameWH.iterrows():
       if not isinstance(row['Stock_Quantity'],int):
           rows_to_drop.append(index)
           print("Row ",index+1," in warehouse file ", whfile, " has a non-numeric Stock Quantity and will be dropped \n")
    dFrameWH = dFrameWH.drop(rows_to_drop)
#
# Remove rows from dFrameWH where product_id is not found in DimProducts
    rows_to_drop = []
    for index, row in dFrameWH.iterrows():
       if not row['product_id'] in dFrame['product_id'].values:
            print("Row ",index+1," in warehouse file ", whfile, " has an invalid product_id and will be dropped \n")
            rows_to_drop.append(index)
    dFrameWH = dFrameWH.drop(rows_to_drop)
# Remove rows from dFrameWH where for a given product_id, the brand_id or category_id information
# is inconsistent with DimProducts
    rows_to_drop = []
    for index, row in dFrameWH.iterrows():
        row_pid = row['product_id']
        prod_table_row_index = dFrame.loc[dFrame['product_id'] == row_pid].index
        subset = dFrame.loc[prod_table_row_index]
        prod_id_ref = subset['product_id'].values
        category_id_ref = subset['category_id'].values
        brand_id_ref = subset['brand_id'].values
        if not row['category_id'] == category_id_ref:
            print('Row ', index+1, ' in warehouse file ',whfile,' for product_id ', row_pid,
                  ' has mis-match on category_id ', category_id_ref, '\n')
            rows_to_drop.append(index)
        if not row['brand_id'] == brand_id_ref:
            print('Row ', index+1, ' in warehouse file ',whfile,' for product_id ', row_pid,
                  ' has mis-match on brand_id ', brand_id_ref, '\n')
            rows_to_drop.append(index)
    dFrameWH = dFrameWH.drop(rows_to_drop)
# Take each row from warehouse excel file dataframe, and retrieve the corresponding record from products dataframe
# Create a session
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
#
    for _, row in dFrameWH.iterrows():
       SD_key = create_Date_Key(row['Stock_Date'])
       if not session.query(DimDate).filter_by(Date_Key=SD_key).first():
           # If Stock_date does not exist in DimDate, add it to DimDate
           addNew_Date(row['Stock_Date'],conn_string_dw)
       new_record = FactInventory(product_id=row['product_id'], Warehouse_ID=row['Warehouse_ID'], Stock_Date=row['Stock_Date'],
                   Stock_Quantity=row['Stock_Quantity'], Stock_Date_Key=SD_key)
       session.add(new_record)
    # Commit the session to persist the changes to the database
    session.commit()
    counter+=1
    print("DataFrame rows successfully inserted into FactInventory table.")
    # Close the session
    session.close()
    ###
    with open('pipe_FactInventory_log.txt', 'a') as f:
            dt = datetime.datetime.now()
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            out_str = f'TimeStamp: {dt_str} --- Number of new customer records loaded into FactInventory = {counter}\n'

            f.write(out_str)
            if len(rows_to_drop) > 0:
                f.write("Dropped Records:\n")
                for index in rows_to_drop:
                    row_index = index + 1  # Adding 1 to match the actual row index
                    record_str = f"Row {row_index} in warehouse file {whfile} was dropped\n"
                    f.write(record_str)
#####################################################################################
pipe_Inventory('Warehouse1.xlsx', conn_string_dw)
pipe_Inventory('Warehouse2.json', conn_string_dw)
#####################################################################################
