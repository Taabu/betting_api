# coding=utf-8
# __author__ = 'Mario Romera Fernández'

import sqlite3
import datetime
from contextlib import closing


DB_NAME = 'BetBright.db'
MATCH_URL = 'http://127.0.0.1:5000/api/v1/resources/match/'


def create_db():
    """
    Creates sqlite3 database
    Tables: SPORTS, EVENTS, MARKETS, SELECTIONS, MESSAGES

    SPORTS
    ------
    Sp_ID: INT PRIMARY KEY sport id
    Sp_NAME: STR sport name

    EVENTS
    ------
    Ev_ID: INT PRIMARY KEY event id
    Ev_NAME: STR event name
    Ev_STARTTIME: INT event start timestamp
    Ev_SPORTID: INT INDEX sport id -> SPORTS.Sp_ID

    MARKETS
    -------
    Ma_ID: INT PRIMARY KEY market id
    Ma_NAME: STR market name
    Ma_EVENTID: INT INDEX event id -> EVENTS.Ev_ID

    SELECTIONS
    ----------
    Se_ID: INT PRIMARY KEY selection id
    Se_NAME: STR selection name
    Se_ODDS: FLOAT selection odds
    Se_MARKETID: INT INDEX market id -> MARKETS.Ma_ID
    Se_EVENTID: INT INDEX event id -> EVENTS.Ev_ID

    MESSAGES
    --------
    Me_ID: INT PRIMARY KEY message id

    :return: None
    """

    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()

        # Drop tables
        cur.execute('''DROP TABLE IF EXISTS SPORTS;''')
        cur.execute('''DROP TABLE IF EXISTS EVENTS;''')
        cur.execute('''DROP TABLE IF EXISTS MARKETS;''')
        cur.execute('''DROP TABLE IF EXISTS SELECTIONS;''')
        cur.execute('''DROP TABLE IF EXISTS MESSAGES;''')

        # Create tables

        # Create table MESSAGES
        cur.execute('''CREATE TABLE IF NOT EXISTS MESSAGES
                       (Me_ID INTEGER PRIMARY KEY);''')

        # Create table SPORTS
        cur.execute('''CREATE TABLE IF NOT EXISTS SPORTS
                       (Sp_ID INTEGER PRIMARY KEY,
                        Sp_NAME text);''')

        # Create table EVENTS
        cur.execute('''CREATE TABLE IF NOT EXISTS EVENTS
                       (Ev_ID INTEGER PRIMARY KEY,
                        Ev_NAME text,
                        Ev_STARTTIME INTEGER,
                        Ev_SPORTID INTEGER,
                        FOREIGN KEY (Ev_SPORTID) REFERENCES SPORTS(Sp_ID));''')

        # Create table MARKETS
        cur.execute('''CREATE TABLE IF NOT EXISTS MARKETS
                       (Ma_ID INTEGER PRIMARY KEY,
                        Ma_NAME text,
                        Ma_EVENTID INTEGER,
                        FOREIGN KEY (Ma_EVENTID) REFERENCES EVENTS(Ev_ID));''')

        # Create table SELECTIONS
        cur.execute('''CREATE TABLE IF NOT EXISTS SELECTIONS
                       (Se_ID INTEGER PRIMARY KEY,
                        Se_NAME text,
                        Se_ODDS FLOAT,
                        Se_MARKETID INTEGER,
                        Se_EVENTID INTEGER,
                        FOREIGN KEY (Se_MARKETID) REFERENCES MARKETS(Ma_ID),
                        FOREIGN KEY (Se_EVENTID) REFERENCES EVENTS(Ev_ID));''')

        # Create indexes

        # Create index on ENVENTS(Ev_SPORTID)
        cur.execute('''CREATE INDEX IF NOT EXISTS evsp_in ON EVENTS(Ev_SPORTID);''')

        # Create index on MARKETS(Ma_EVENTID)
        cur.execute('''CREATE INDEX IF NOT EXISTS maev_in ON MARKETS(Ma_EVENTID);''')

        # Creater indexes on SELECTIONS(Se_MARKETID, Se_EVENTID)
        cur.execute('''CREATE INDEX IF NOT EXISTS sema_in ON SELECTIONS(Se_MARKETID);''')
        cur.execute('''CREATE INDEX IF NOT EXISTS seev_in ON SELECTIONS(Se_EVENTID);''')

        # Commit
        conn.commit()


def show_db():
    """
    Prints database info
    :return: None
    """

    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()
        cur.execute('''SELECT name FROM sqlite_master WHERE type='table';''')
        tables = cur.fetchall()
        for table in tables:
            print(table)
            cur.execute('''SELECT * FROM %s;''' % table)
            data = cur.fetchall()
            print(data)


def message_processed(message_id):
    """
    Checks if a message has been already processed
    :param message_id: INT message id
    :return: BOOLEAN True if it has been processed, False if not
    """

    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()
        cur.execute('''SELECT Me_ID FROM MESSAGES
                       WHERE Me_ID = '%i';''' % message_id)
        results = cur.fetchall()
    if len(results) == 1:
        return True
    else:
        return False


def insert_processed_message(message_id):
    """
    Inserts a message id in messages table if it has been processed
    :param message_id: INT message id
    :return: None
    """

    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()

        # Insert message_id in messages table
        cur.execute('''INSERT INTO MESSAGES VALUES('%i');''' % message_id)

        # Commit
        conn.commit()


def new_event(full_event_info):
    """
    Populates database tables with the new event data provided by external data provider
    :param full_event_info: DICT event data received from external provider
    :return: None
    """

    event = full_event_info['event']
    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()

        # Insert sport
        cur.execute('''INSERT OR IGNORE INTO SPORTS VALUES('%i', '%s');''' %
                    (event['sport']['id'], event['sport']['name']))

        # Insert event
        event_time = datetime.datetime.strptime(event['startTime'], "%Y-%m-%d %H:%M:%S").timestamp()
        cur.execute('''INSERT INTO EVENTS VALUES('%i', '%s', '%i', '%i');''' %
                    (event['id'], event['name'], event_time, event['sport']['id']))

        # Insert markets
        for market in event['markets']:
            cur.execute('''INSERT INTO MARKETS VALUES('%i', '%s', '%i');''' %
                        (market['id'], market['name'], event['id']))

            # Insert selections
            for selection in market['selections']:
                cur.execute('''INSERT INTO SELECTIONS VALUES('%i', '%s', '%f', '%i', '%i');''' %
                            (selection['id'], selection['name'], selection['odds'], market['id'], event['id']))

        # Commit
        conn.commit()

    # Insert message id in messages table
    insert_processed_message(full_event_info['id'])


def update_odds(update_event_info):
    """
    Updates odds from data received from external data provider
    :param update_event_info: DICT update odds data from external provider
    :return: None
    """

    event = update_event_info['event']
    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()
        for market in event['markets']:
            for selection in market['selections']:
                cur.execute('''UPDATE SELECTIONS SET Se_ODDS = '%f' WHERE
                            Se_ID = '%i' AND Se_MARKETID = '%i' AND Se_EVENTID = '%i';''' %
                            (selection['odds'], selection['id'], market['id'], event['id']))

        # Commit
        conn.commit()

    # Insert message id in messages table
    insert_processed_message(update_event_info['id'])


def get_match_by_name(match_name):
    """
    Get match data by name
    :param match_name: STR match name
    :return: DICT match data
    """

    matches = []
    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()
        cur.execute('''SELECT Ev_ID, Ev_STARTTIME FROM EVENTS WHERE Ev_NAME = '%s';''' % match_name)
        results = cur.fetchall()
    for match in results:
        match_info = {'id': match[0],
                      'url': 'http://127.0.0.1:5000/api/v1/resources/match/%i' % match[0],
                      'name': match_name,
                      'startTime': datetime.datetime.fromtimestamp(match[1]).strftime("%Y-%m-%d %H:%M:%S")}
        matches.append(match_info)
    return matches


def get_match_by_id(match_id):
    """
    Get match data by id
    :param match_id: INT match id
    :return: DICT match data
    """

    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()
        cur.execute('''SELECT Ev_ID, Ev_NAME, Ev_STARTTIME, Ev_SPORTID, Sp_NAME, Ma_ID, Ma_NAME, Se_ID, Se_NAME, Se_ODDS
                       FROM EVENTS
                       INNER JOIN SPORTS ON SPORTS.Sp_ID = EVENTS.Ev_SPORTID
                       INNER JOIN MARKETS ON MARKETS.Ma_EVENTID = EVENTS.Ev_ID
                       INNER JOIN SELECTIONS ON SELECTIONS.Se_EVENTID = EVENTS.Ev_ID
                                            AND SELECTIONS.Se_MARKETID = MARKETS.Ma_ID
                       WHERE Ev_ID = '%i';''' % match_id)
        results = cur.fetchall()

    # Common data to every result from database
    match_name = results[0][1]
    match_starttime = results[0][2]
    match_sport_id = results[0][3]
    match_sport_name = results[0][4]
    markets_list = []

    # Set of markets
    markets = set([(x[5], x[6]) for x in results])
    for market in markets:
        selection_list = []

        # List of selections
        selections = [(x[7], x[8], x[9]) for x in results if (x[5], x[6]) == market]
        for selection in selections:
            selection_list.append({'id': selection[0],
                                   'name': selection[1],
                                   'odds': selection[2]})
        markets_list.append({'id': market[0],
                             'name': market[1],
                             'selections': selection_list})

    # Populate DICT with match data
    match_info = {'id': match_id,
                  'url': '%s%i' % (MATCH_URL, match_id),
                  'name': match_name,
                  'startTime': datetime.datetime.fromtimestamp(match_starttime).strftime("%Y-%m-%d %H:%M:%S"),
                  'sport': {'id': match_sport_id,
                            'name': match_sport_name},
                  'markets': markets_list
                  }
    return match_info


def get_football_matches_by_start_time():
    """
    Get football matches by start time
    :return: DICT macthes data
    """

    with closing(sqlite3.connect(DB_NAME)) as conn:
        cur = conn.cursor()
        cur.execute('''SELECT Sp_ID FROM SPORTS WHERE Sp_NAME = 'Football';''')
        sport_id = cur.fetchall()[0]
        cur.execute('''SELECT Ev_ID, Ev_NAME, Ev_STARTTIME FROM EVENTS
                       WHERE Ev_SPORTID = '%i' ORDER BY Ev_STARTTIME ASC;''' % sport_id)
        results = cur.fetchall()

    # Populate list with matches data
    matches = []
    for match in results:
        match_id, match_name, match_start_time = match
        match_info = {'id': match_id,
                      'url': '%s%i' % (MATCH_URL, match_id),
                      'name': match_name,
                      'startTime': datetime.datetime.fromtimestamp(match_start_time).strftime("%Y-%m-%d %H:%M:%S")}
        matches.append(match_info)
    return matches

if __name__ == '__main__':

    # create_db()
    show_db()
