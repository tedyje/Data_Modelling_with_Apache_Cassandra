#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import glob
import json
import csv
from cql_queries import *
from connect import connect
from time import sleep

def combine_csv_files(subfolder = None):

    current_directory = os.getcwd()
    print(f"The current working subfolder is: {current_directory}")

    if subfolder:
        filePathList = glob.glob(os.path.join(current_directory, subfolder, "**", "*"), recursive = True)

    else:
        filePathList = glob.glob(os.path.join(current_directory, "**", "*"), recursive = True)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    print("Processing csv files...")
    print("-----------------------")
    sleep(2)
    
    # for every csv file path
    i, n = 0, len(filePathList)
    for filePath in filePathList:

        # reading csv file with csv module
        with open (filePath, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)

            full_data_rows_list.extend(list(csv_reader))
        i += 1
        print('{}/{} files processed.'.format(i, n))

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf-8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, dialect = "myDialect")

        csv_writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
            
        
        # Collect the records having artists not null to record_list
        record_list = []
        for row in full_data_rows_list:

            if row[0] == '':
                continue
            
            # Write the data to csv file
            csv_writer.writerow((row[0], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[12], row[13], row[16]))
            record_list.append([row[0], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[12], row[13], row[16]])
    
    print("Finished processing csv files!!\n")
    return record_list


def load_data(record_list, session):

    n = len(record_list)
    i,j,k = 0, 0, 0;
    # Insert the records into session_songs table
    print("Inserting rows to tables")
    print("------------------------")
    sleep(2)
    
    for record in record_list:
        
        # Assigned each fields value to field name variables
        artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId = record
        session.execute(sessionsongs_table_insert, (int(sessionId), int(itemInSession), artist, song, float(length)))
        
        i += 1
        print(f"Inserting rows to session_songs table:{i}/{n}", end = "\r")
   
    print("Finished inserting rows to session_songs table!!")
   
    # Insert the records into user_songs table
    for record in record_list:
        
        # Assigned each fields value to field name variables
        artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId = record
        session.execute(usersongs_table_insert, (int(userId), int(sessionId), artist, song, firstName, lastName, int(itemInSession)))
        
        j += 1
        print(f"Inserting rows to user_songs table:{j}/{n}", end = "\r")
        
    print("Finished inserting rows to user_songs table!!")
    
    # Insert the records into app_history table
    for record in record_list:
        
        # Assigned each fields value to field name variables
        artist, firstName, gender, itemInSession, lastName, length, level, location, sessionId, song, userId = record
        session.execute(apphistory_table_insert, (song, firstName, lastName, int(userId)))
        
        k += 1
        print(f"Inserting rows to app_history table:{k}/{n}", end = "\r")
        
    print("Finished inserting rows to app_history table!!")  

if __name__ == "__main__":
    
    cluster, session = connect()
    record_list = combine_csv_files(subfolder = "event_data")
    load_data(record_list, session)
    
    print("\nClosing Cluster!!")
    print("-------------------")
    sleep(2)
    
    cluster.shutdown()
    session.shutdown()
    
    print("Cluster Closed!!")
    print("Session Closed!!")
    
