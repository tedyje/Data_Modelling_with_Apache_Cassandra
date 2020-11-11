### Data Modeling with Cassandra

Log data file collected on songs and user activity by *sparkify* through their music streaming app. Created Apache Cassandra database which can create queries on song play data to answer some questions

**Dataset**: The directory *event_data* contains 30 log CSV files partitioned by date. Each csv files have different number of records. And each csv file has same fields name:

      >'artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
       'length', 'level', 'location', 'method', 'page', 'registration',
       'sessionId', 'song', 'status', 'ts', 'userId'

#### Modeling NoSQL database or Apache Cassandra database

1. Records of 30 log *csv* files in event_data folder were combined together and saved to a single event_datafile_new.csv file. The orginal fields' name were modified to the following fields name.
      >- artist
      >- firstName of user
      >- gender of user
      >- item number in session
      >- last name of user
      >- length of the song
      >- level (paid or free song)
      >- location of the user
      >- sessionId
      >- song title
      >- userId

2. Created denormalized dataset after processing event_datafile_new.csv
3. Tables with parititon and clustering columns were Created
6. Records in *event_datafile_new.csv* was processed and inserted to the tables

**Running**
1. Run first create_table.py file
2. Run etl.py next
