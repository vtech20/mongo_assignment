import pymongo
import pandas as pd
import logging
import os
import urllib
import json

def read_file(filepath,delimter):
    """This function will read the CSV file and return a dataframe"""
    try:
        if os.path.splitext(filepath)[1] == ".csv":
            df = pd.read_csv(filepath,sep = delimter)
            logging.info("Successfully read the file")
    except urllib.error.URLError as u:
        df = pd.DataFrame()
        logging.exception("The URL is incorrect or coruppted. Check the url" + str(u))
    except Exception as e:
        df = pd.DataFrame()
        logging.exception("Exception Occured while reading file :" + str(e.reason))
    return df


def checkExistence_COL(COLLECTION_NAME, DB_NAME, db):
    """It verifies the existence of collection name in a database"""
    try:
        collection_list = db.list_collection_names()

        if COLLECTION_NAME in collection_list:
            logging.info("Collection:" + COLLECTION_NAME + " in Database:" + DB_NAME +" exists")
            return True
        else:
            logging.info("Collection:" + COLLECTION_NAME +" in Database:" + DB_NAME + " does not exists OR \n no documents are present in the collection")
            return False
    except Exception as e:
        logging.exception("Exception Occured while checking collection :" + str(e.reason))

def get_connection(connection_string,db_name,collection_name):
    """This function is to get the connection to MongoDB and return the collection"""
    try:
        client = pymongo.MongoClient(connection_string)
        db = client.db_name
        coll_valid = checkExistence_COL(collection_name,db_name,db)
        if coll_valid:
            collection = db[collection_name]
        else:
            collection = db[collection_name]
        logging.info("Collection has been setup")
    except Exception as e:
        logging.exception("Exception Occured while getting connection :" + str(e.reason))
    return collection

def insert_DB_file(df,collection):
    """This is the function to insert data into Mongo DB from file"""
    try:
        payload = json.loads(df.to_json(orient='records'))
        collection.insert_many(payload)
        logging.info("Successfully inserted the document into DB")
    except Exception as e:
        logging.exception("Exception Occured while inserting the documents :" + str(e.reason))

def drop_col(collection):
    """This is the function to drop the collection"""
    try:
        collection.drop()
        logging.info("Successfully dropped the collection from DB")
    except Exception as e:
        logging.exception("Exception Occured while dropping the collection :" + str(e.reason))