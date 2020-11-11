#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from time import sleep


def connect():
    """
    Establish cluster connection and return's the cluster and session references.
    :return: return's (cur, conn) a cursor and connection
    """
    conn = None

    try:

        # connect to a Cassandra server
        print("Connecting to the Apache Cassandra cluster ...")
        cluster = Cluster(['127.0.0.1'])
     
        # Create a session
        session = cluster.connect()
        
        sleep(2)
        print("Connection Established!!\n")

    except Exception as e:
        print(f"Connection Failed !! Error : {e}")

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    session.set_keyspace('sparkify')


    return cluster, session
