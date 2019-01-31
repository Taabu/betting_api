# coding=utf-8
# __author__ = 'Mario Romera Fernández'

import unittest
import requests
import sqlite3
from contextlib import closing
import db


class TestCase(unittest.TestCase):
    def test_0_create_database(self):
        db.create_db()
        with closing(sqlite3.connect(db.DB_NAME)) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT name FROM sqlite_master WHERE type='table';''')
            tables = cur.fetchall()
        assert ('MESSAGES',) and ('SPORTS',) and ('EVENTS',) and ('MARKETS',) and ('SELECTIONS',) in tables

    def test_1_new_event(self):
        url = 'http://127.0.0.1:5000/api/v1/resources/external/'
        headers = {'Content-Type': 'application/json'}
        data = open('newevent.json', 'rb').read()
        response = requests.post(url=url, data=data, headers=headers)
        with closing(sqlite3.connect(db.DB_NAME)) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT * FROM MESSAGES;''')
            messages = cur.fetchall()
            cur.execute('''SELECT * FROM SPORTS;''')
            sports = cur.fetchall()
            cur.execute('''SELECT * FROM EVENTS;''')
            events = cur.fetchall()
            cur.execute('''SELECT * FROM MARKETS;''')
            markets = cur.fetchall()
            cur.execute('''SELECT * FROM SELECTIONS;''')
            selections = cur.fetchall()
        assert '<p>200</p>' in response.text
        assert (8661032861909884000,) in messages
        assert (221, 'Football') in sports
        assert (994839351740, 'Real Madrid vs Barcelona', 1529487000, 221) in events
        assert (385086549360973300, '1st Half Winner', 994839351740) and (
                385086549360973400, 'Winner', 994839351740) in markets
        assert (1243901714083343600, 'Real Madrid', 1.01, 385086549360973300, 994839351740) and (
                1737666888266680800, 'Barcelona', 1.01, 385086549360973300, 994839351740) and (
                2737666888266680800, 'Draw', 2.0, 385086549360973300, 994839351740) and (
                5737666888266680000, 'Barcelona', 1.01, 385086549360973400, 994839351740) and (
                6737666888266680000, 'Draw', 2.0, 385086549360973400, 994839351740) and (
                8243901714083343000, 'Real Madrid', 1.01, 385086549360973400, 994839351740) in selections

    def test_2_update_odds(self):
        url = 'http://127.0.0.1:5000/api/v1/resources/external/'
        headers = {'Content-Type': 'application/json'}
        data = open('updateodds.json', 'rb').read()
        response = requests.put(url=url, data=data, headers=headers)
        with closing(sqlite3.connect(db.DB_NAME)) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT * FROM MESSAGES;''')
            messages = cur.fetchall()
            cur.execute('''SELECT * FROM SELECTIONS;''')
            selections = cur.fetchall()
        assert '<p>200</p>' in response.text
        assert (8661032861909889000,) in messages
        assert (5737666888266680000, 'Barcelona', 5.55, 385086549360973400, 994839351740) and (
                6737666888266680000, 'Draw', 1.01, 385086549360973400, 994839351740) and (
                8243901714083343000, 'Real Madrid', 10.0, 385086549360973400, 994839351740) in selections

    def test_3_new_event_2(self):
        url = 'http://127.0.0.1:5000/api/v1/resources/external/'
        headers = {'Content-Type': 'application/json'}
        data = open('newevent2.json', 'rb').read()
        response = requests.post(url=url, data=data, headers=headers)
        with closing(sqlite3.connect(db.DB_NAME)) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT * FROM MESSAGES;''')
            messages = cur.fetchall()
            cur.execute('''SELECT * FROM SPORTS;''')
            sports = cur.fetchall()
            cur.execute('''SELECT * FROM EVENTS;''')
            events = cur.fetchall()
            cur.execute('''SELECT * FROM MARKETS;''')
            markets = cur.fetchall()
            cur.execute('''SELECT * FROM SELECTIONS;''')
            selections = cur.fetchall()
        assert '<p>200</p>' in response.text
        assert (8661032861909884001,) in messages
        assert (221, 'Football') in sports
        assert (994839351741, 'Boca Juniors vs River Plate', 1529523000, 221) in events
        assert (385086549360973301, '1st Half Winner', 994839351741) and (
                385086549360973401, 'Winner', 994839351741) in markets
        assert (1243901714083343601, 'Boca Juniors', 1.01, 385086549360973301, 994839351741) and (
                1737666888266680801, 'River Plate', 1.01, 385086549360973301, 994839351741) and (
                2737666888266680801, 'Draw', 2.0, 385086549360973301, 994839351741) and (
                5737666888266680001, 'River Plate', 1.01, 385086549360973401, 994839351741) and (
                6737666888266680001, 'Draw', 2.0, 385086549360973401, 994839351741) and (
                8243901714083343001, 'Boca Juniors', 1.01, 385086549360973401, 994839351741) in selections

    def test_4_match_by_id(self):
        url = 'http://127.0.0.1:5000/api/v1/resources/match/994839351740'
        response = requests.get(url=url).json()
        assert 994839351740 == response['id']
        assert "Real Madrid vs Barcelona" in response['name']
        assert 221 == response['sport']['id']
        assert "Football" in response['sport']['name']
        assert "2018-06-20 10:30:00" in response['startTime']
        assert "http://127.0.0.1:5000/api/v1/resources/match/994839351740" in response['url']
        for market in response['markets']:
            assert 385086549360973300 or 385086549360973400 == market['id']
            assert "1st Half Winner" or "Winner" in market['name']
            for selection in market['selections']:
                {"id": 1243901714083343600, "name": "Real Madrid", "odds": 1.01} or {
                 "id": 1737666888266680800, "name": "Barcelona", "odds": 1.01} or {
                 "id": 2737666888266680800, "name": "Draw", "odds": 2.0} or {
                 "id": 5737666888266680000, "name": "Barcelona", "odds": 5.55} or {
                 "id": 6737666888266680000, "name": "Draw", "odds": 1.01} or {
                 "id": 8243901714083343000, "name": "Real Madrid", "odds": 10.0} in selection

    def test_5_match_by_time(self):
        url = 'http://127.0.0.1:5000/api/v1/resources/match/football/?ordering=startTime'
        response = requests.get(url=url).json()
        assert [{"id": 994839351740,
                 "name": "Real Madrid vs Barcelona",
                 "startTime": "2018-06-20 10:30:00",
                 "url": "http://127.0.0.1:5000/api/v1/resources/match/994839351740"},
                {"id": 994839351741,
                 "name": "Boca Juniors vs River Plate",
                 "startTime": "2018-06-20 20:30:00",
                 "url": "http://127.0.0.1:5000/api/v1/resources/match/994839351741"}] == response

    def test_6_match_by_name(self):
        url = 'http://127.0.0.1:5000/api/v1/resources/match/?name=Real%20Madrid%20vs%20Barcelona'
        response = requests.get(url=url).json()
        assert [{"id": 994839351740,
                 "name": "Real Madrid vs Barcelona",
                 "startTime": "2018-06-20 10:30:00",
                 "url": "http://127.0.0.1:5000/api/v1/resources/match/994839351740"}] == response


if __name__ == '__main__':
    unittest.main()
