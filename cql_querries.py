#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# DROP TABLES
drop_sessionongs = "DROP TABLE session_songs"
drop_usersongs = "DROP TABLE songs_table"
drop_apphistory = "DROP TABLE app_history"

# CREATE TABLES
create_session_songs = ("""
    CREATE TABLE IF NOT EXISTS session_songs
    (sessionId int, itemInSession int, artist text, song_title text, song_length float,
    PRIMARY KEY(sessionId, itemInSession))
    """)

create_user_songs = ("""
    CREATE TABLE IF NOT EXISTS user_songs
    (userId int, sessionId int, artist text, song text, firstName text, lastName text, itemInSession int,
    PRIMARY KEY((userId, sessionId), itemInSession))
    """)


create_app_history_table = ("""
    CREATE TABLE IF NOT EXISTS app_history
    (song text, firstName text, lastName text, userId int,
    PRIMARY KEY(song, userId))
    """)

# INSERT RECORDS

sessionsongs_table_insert = ("""NSERT INTO session_songs (sessionId, itemInSession, artist, song_title, song_length)
                     VALUES (%s, %s, %s, %s, %s, %s)
                     """)

usersongs_table_insert = ("""INSERT INTO user_songs
                         (userId, sessionId, artist, song, firstName, lastName, itemInSession)
                         (%s, %s, %s, %s, %s, %s, %s)
                         """)

apphistory_table_insert = ("""INSERT INTO app_history (song, firstName, lastName, userId)
                     VALUES (%s, %s, %s, %s)
                     """)
