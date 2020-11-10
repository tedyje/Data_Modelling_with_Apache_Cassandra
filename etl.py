import pandas as pd
import os
import glob
import re
import numpy as np
import json
import csv

def combine_csv_files(subfolder = None):

    current_directory = os.getcwd()
    print(f"The current working subfolder is: {current_directory}")

    if subfolder:
        filePathList = glob.glob(os.path.join(current_directory, subfolder, "**", "*"), recursive = True)

    else:
        fileList = glob.glob(os.path.join(current_directory, "**", "*"), recursive = True)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every csv file path
    for filePath in filePathList:

        # reading csv file with csv module
        with open (filePath, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)

            full_data_rows_list.extend(list(csv_reader))

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf-8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file, dialect = "myDialect")

        csv_writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])

        for row in full_data_rows_list:

            if row[0] == '':
                continue

            csv_writer.writerow((row[0], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[12], row[13], row[16]))

    return full_data_rows_list




if __name__ == "__main__":

    fileData = combine_csv_files("event_data")
    print(len(fileData))

    with open('event_datafile_new.csv', 'r') as f:
        print(sum(1 for _ in f))
