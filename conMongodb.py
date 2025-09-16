import pymongo
from tabulate import tabulate
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# List the available databases
# print("Available databases:", client.list_database_names())

def find_query(dbName, collectionName, query, projection, sort_col):
    db = client[dbName]  # Specify the database name
    collection = db[collectionName]  # Specify the collection name
    query_output = collection.find(query, projection).sort(sort_col)
    documents = list(query_output)
    df = pd.DataFrame(documents)
    column_names = list(collection.find_one(query, projection).keys())
    result_table = tabulate(df, headers = column_names, tablefmt="grid", showindex="False")
    return result_table
#
def aggregate_query(dbName, collectionName, pipeline):
    db = client[dbName]  # Specify the database name
    collection = db[collectionName]  # Specify the collection name
    pipe_output = collection.aggregate(pipeline)
    first_document = next(pipe_output, None)  # Get the first document or None if no documents
    if first_document:
        column_names = list(first_document.keys())
    else:
        print("No documents found")
    documents = list(pipe_output)
    df = pd.DataFrame(documents)
    result_table = tabulate(df, headers = column_names, tablefmt="grid", showindex="False")
    return result_table
#
db = 'DreamHome'
collection = 'Staff'
#
#
# Execute a MongoDB NoSQL find() query
# Define the query criteria
query = {"$and": [{"salary": {"$gte": 20000}}, {"salary": {"$lte": 30000}}]}
# Define projection to include only certain fields
projection = {"_id": 0, "staffno": 1, "fname": 1, "lname": 1, "position": 1, "salary": 1}
# Specify any Sort
sort_col = {"salary" : -1}
# Call the query output function
result_table = find_query(db, collection, query, projection, sort_col)
# Print Result
print(result_table)
#
#
# Call an aggregation pipeline
pipeline = [
    {"$group": {"_id": "$branchno", "brancho": {"$first": "$branchno"},
                "myCount": {"$sum": 1}, "mySum": {"$sum": "$salary"}}},
         {"$project": { "_id": 0}}, {"$sort": { "branchno": -1}}
        ]
result_table = aggregate_query(db, collection, pipeline)
# Print Result
print(result_table)

