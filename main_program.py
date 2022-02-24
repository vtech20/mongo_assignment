from mongo_pkg.mongo_func import *
import pymongo
import pandas as pd
import logging
import os
import urllib
import json

logging.basicConfig(level=logging.DEBUG,filename="log_mongo_assignment.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
try:
    #Read the file
    logging.info("Started reading the file")
    df = read_file("https://archive.ics.uci.edu/ml/machine-learning-databases/00448/carbon_nanotubes.csv",";")

    #get the collection
    logging.info("started getting connection")
    collection = get_connection("mongodb+srv://mongodb:mongodb@cluster0.xs8xl.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",'mongo_nanotubes','nanotubes')

    #insert file data to DB
    logging.info("Started inserting the file to DB")
    dd = insert_DB_file(df,collection)

    #find the documents
    logging.info("Lets get started with finding the data in collection")
    logging.info("Find first one")
    logging.info(str(collection.find_one()))
    logging.info("Find all")
    for i in collection.find():
        logging.info(str(i))
    logging.info("find the documents which have 'Chiral indice n' is having 12")
    for i in collection.find({"Chiral indice n":12}):
        logging.info(str(i))


    #update data into the document
    logging.info("started updating the document")
    logging.info("update the document with 'Chiral indice n' to 3 where it is 2")
    collection.update_many({"Chiral indice n" : 2},{"$set" : {"Chiral indice n" : 3}})
    logging.info("Successfully updated")

    #drop the collection
    logging.info("Started dropping the collection")
    drop_col(collection)

except Exception as e:
    logging.exception("Exeception occured during the main execution" + str(e))