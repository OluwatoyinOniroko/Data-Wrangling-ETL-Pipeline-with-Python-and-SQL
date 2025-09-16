import pandas as pd
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
def pipe_Customer(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = f'SELECT * FROM customers'
    dFrame = pd.read_sql_query(sqlQuery, engine_db)
#
#
#   This is ORM whereby an object is created corresponding to the database table DimCustomers
    Base = declarative_base()
    class DimCustomers(Base):
        __tablename__ = 'DimCustomers'
#   The ORM DimCustomers object bound to the database table DimCustomers
        customer_id = Column(Integer, primary_key=True, autoincrement=False)
        first_name = Column(String)
        last_name  = Column(String)
        phone = Column(String)
        email = Column(String)
        street = Column(String)
        city = Column(String)
        state = Column(String)
        zip_code = Column(Integer)
#
# Create an ORM session connected to the dimensional database MyStoreDWETL using the connection-string conStrdw
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
    for _, row in dFrame.iterrows():
       if not session.query(DimCustomers).filter_by(customer_id=row['customer_id']).first():
            # If primary key does not exist customer_id from dFrame (customers table) is absent in DimCustomers
            #, create a new object and add it to the session
            new_record = DimCustomers(customer_id=row['customer_id'], first_name=row['first_name'], last_name=row['last_name'],
                                  phone=row['phone'], email=row['email'], street=row['street'], city=row['city'], state=row['state'],
                                  zip_code=row['zip_code'])
            session.add(new_record)
            counter += 1
       else:
            print(f"Skipping duplicate primary key: {row['customer_id']}")
    # Commit the session to persist the changes to the database
    session.commit()
    # Close the session
    session.close()
###
    with open('pipe_customers_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new customer records loaded into DimCustomers = ' + str(counter)
        f.write(out_str)

#####################################################################################
pipe_Customer(conn_string_db, conn_string_dw)
#####################################################################################
