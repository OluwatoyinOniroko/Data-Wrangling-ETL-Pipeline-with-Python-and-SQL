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
    sqlQuery = """
Select p.product_id, p.product_name, p.model_year, p.category_id, c.category_name, p.brand_id, b.brand_name
from products p
Join categories c on p.category_id = c.category_id
Join brands b on p.brand_id = b.brand_id
"""
    dFrame = pd.read_sql_query(sqlQuery, engine_db)
    #
    #
    #   This is ORM whereby an object is created corresponding to the database table DimProducts
    Base = declarative_base()

    class DimProducts(Base):
        __tablename__ = 'DimProducts'
        #   The ORM DimCustomers object bound to the database table DimCustomers
        product_id = Column(Integer, primary_key=True, autoincrement=False)
        product_name = Column(String)
        model_year = Column(Integer)
        category_id = Column(Integer)
        category_name = Column(String)
        brand_id = Column(Integer)
        brand_name = Column(String)

    #
    # Create an ORM session connected to the dimensional database MyStoreDWETL using the connection-string conStrdw
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0
    for _, row in dFrame.iterrows():
        if not session.query(DimProducts).filter_by(product_id=row['product_id']).first():
            # If primary key does not exist customer_id from dFrame (product table) is absent in DimCustomers
            # , create a new object and add it to the session
            new_record = DimProducts(product_id=row['product_id'],
                                     product_name=row['product_name'],
                                     model_year=row['model_year'],
                                     category_id=row['category_id'],
                                     category_name=row['category_name'],
                                     brand_id=row['brand_id'],
                                     brand_name=row['brand_name'])
            session.add(new_record)
            counter += 1
        else:
            print(f"Skipping duplicate primary key: {row['product_id']}")
    # Commit the session to persist the changes to the database
    session.commit()
    # Close the session
    session.close()
    ###
    with open('pipe_products_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = 'TimeStamp: ' + dt_str + ' --- Number of new customer records loaded into DimProducts = ' + str(
            counter)
        f.write(out_str)


#####################################################################################
pipe_Customer(conn_string_db, conn_string_dw)
#####################################################################################
