import pandas as pd
import json
import os
import pyodbc
import urllib
import datetime
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
def create_Date_Key_df(inFrame, column_name):
    in_Year = pd.to_datetime(inFrame[column_name]).dt.year
    in_Month = pd.to_datetime(inFrame[column_name]).dt.strftime('%m')
    in_DayNumber = pd.to_datetime(inFrame[column_name]).dt.strftime('%d')
    out_Date_Key = (in_Year.astype(str) + in_Month.astype(str) + in_DayNumber.astype(str)).astype(int)
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
#   print(chk_Date_Key, chk_Date, chk_Year, chk_Month, chk_MonthName, chk_Week, chk_WeekDay, chk_WeekDayName, chk_DayNumber)    
#
# Create a session
    Session = sessionmaker(bind=engine_dw)
    session = Session()
#
    if not session.query(DimDate).filter_by(Date_Key=chk_Date_Key).first():
        # If primary key does not exist, create a new object and add it to the session
          new_record = DimDate(Date_Key=chk_Date_Key, Date = chk_Date, Year = chk_Year, Month = chk_Month, MonthName = chk_MonthName,
                              Week=chk_Week, WeekDay=chk_WeekDay, WeekDayName=chk_WeekDayName, DayNumber = chk_DayNumber)
          session.add(new_record)
        # Commit the session to persist the changes to the database
          session.commit()
          print("Date rows successfully inserted into SQL Server table.")
        # Close the session
          session.close()
    else:
          print(f"Skipping duplicate primary key:", chk_Date_Key)

#####################################################################################
def pipe_Sales(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = """SELECT o.order_id, customer_id, [order_date], [required_date], o.shipped_date,[item_id], [product_id], [quantity], [list_price], [discount]
    FROM orders o, order_items oi
    WHERE o.[order_id] = oi.[order_id] AND o.shipped_date IS NOT NULL"""
    dFrame = pd.read_sql_query(sqlQuery, engine_db)
#
    engine_dw = create_engine(conStrdw)
#
    Base = declarative_base()
    class FactSales(Base):
        __tablename__ = 'FactSales'
        TransID = Column(Integer, primary_key=True, autoincrement=True)
        product_id = Column(Integer)
        Order_Date_Key = Column(Integer)
        Required_Date_Key = Column(Integer)
        Shipped_Date_Key = Column(Integer)
        order_id = Column(Integer)
        item_id = Column(Integer)
        customer_id = Column(Integer)
        order_date  = Column(DateTime)
        required_date = Column(DateTime)
        shipped_date = Column(DateTime)
        quantity = Column(Integer)
        list_price = Column(Float)
        discount = Column(Float)
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
#    
    dFrame['order_date_key'] = create_Date_Key_df(dFrame, 'order_date')
    dFrame['required_date_key'] = create_Date_Key_df(dFrame, 'required_date')
    dFrame['shipped_date_key'] = create_Date_Key_df(dFrame, 'shipped_date')
# Create a session
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
#
    for _, row in dFrame.iterrows():
       if not session.query(DimDate).filter_by(Date_Key=row['order_date_key']).first():
            # If Order date does not exist in DimDate, add it to DimDate
            addNew_Date(row['order_date'],conn_string_dw)
       if not session.query(DimDate).filter_by(Date_Key=row['required_date_key']).first():
            # If Required date does not exist in DimDate, add it to DimDate
            addNew_Date(row['required_date'],conn_string_dw)
       if not session.query(DimDate).filter_by(Date_Key=row['shipped_date_key']).first():
            print("Key not found", row['shipped_date_key']) 
            # If Shipped date does not exist in DimDate, add it to DimDate
            addNew_Date(row['shipped_date'],conn_string_dw)
    for _, row in dFrame.iterrows():
        print(row['order_id'])
        if not session.query(FactSales).filter_by(order_id=row['order_id']).filter_by(customer_id=row['customer_id']).filter_by(item_id=row['item_id']).first():
            # If order_id does not exist in FactSales, create a new object and add it to the session
            new_record = FactSales(product_id=row['product_id'], order_id=row['order_id'], item_id=row['item_id'], customer_id=row['customer_id'],
                                   Order_Date_Key=row['order_date_key'], Required_Date_Key=row['required_date_key'], Shipped_Date_Key=row['shipped_date_key'],
                                   order_date=row['order_date'].strftime("%Y-%m-%d"),
                                   required_date=row['required_date'].strftime("%Y-%m-%d"),
                                   shipped_date=row['shipped_date'].strftime("%Y-%m-%d"),
                                   quantity=row['quantity'], list_price=row['list_price'], discount=row['discount'])
            session.add(new_record)
            session.commit()
            counter += 1
        else:
            print(f"Skipping duplicate order id: ", row['order_id'])
    # Commit the session to persist the changes to the database
    session.commit()
    print("DataFrame rows successfully inserted into SQL Server table.")
    # Close the session
    session.close()
    ###
    with open('pipe_Factsales_log.txt', 'w') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new customer records loaded into FactSales = ' + str(
            counter)
        f.write(out_str)
#####################################################################################
pipe_Sales(conn_string_db, conn_string_dw)
#####################################################################################
