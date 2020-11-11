#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from cql_queries import drop_table_queries, create_table_queries
from connect import connect


def drop_tables(session):
    """
    Run's all the drop table queries defined in sql_queries.py
    :param cur: cursor to the database
    :param conn: database connection reference

    """
    i = 0
    for query in drop_table_queries:
        session.execute(query)
        i += 1

    print(f"{i} Tables dropped successfully!!")

def create_tables(session):
    
    """
    Run's all the create table queries defined in sql_queries.py
    :param cur: cursor to the database
    :param conn: database connection reference

    """
    i = 0
    for query in create_table_queries:
        session.execute(query)
        i += 1
    
    print(f"{i} Tables were created successfully!!")
        
        
def main():
    
    cluster, session = connect()
    
    drop_tables(session)
    create_tables(session)
    
    session.shutdown()
    cluster.shutdown()
    
    print("\nConnection closed!!")
    
if __name__ == "__main__":
    main()