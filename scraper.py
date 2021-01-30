import sqlite3
from pathlib import Path
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import time
import datetime
import re

class DB():
    def __init__(self, path: Path, db: str, table: str):
        self.path = path
        self.dbname = db
        self.tablename = table
        self.connection = self.connect()
        self.connection.execute(f"CREATE TABLE IF NOT EXISTS {self.tablename} (id INTEGER PRIMARY KEY, time INTEGER, surface FLOAT, volume FLOAT, inflow FLOAT, drain FLOAT, rainfall FLOAT, temperature FLOAT)")
        self.commit()

    def connect(self):
        con = sqlite3.connect(self.path / self.dbname)
        return con

    def commit(self):
        self.connection.commit()

    def save(self, data):
        cursor = self.connection.cursor()
        res = cursor.execute(f"SELECT * FROM {self.tablename} WHERE time=?", (data[0], ))
        res = res.fetchone()
        if res is None:
            self.connection.execute(f"INSERT INTO {self.tablename} (time, surface, volume, inflow, drain, rainfall, temperature) VALUES(?,?,?,?,?,?,?)", tuple(data))
            self.commit()

    def close(self):
        self.connection.close()

class Scraper():
    def __init__(self, url):
        self.url = url

    def getHeader(self):
        ua = UserAgent()
        header = {"User-Agent": ua.random}
        return header

    def getHTML(self, header, url):
        res = requests.get(url, headers = header)
        if res.status_code == 200:
            html = res.content
        return html

    def parseHTML(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.find_all(text=True)

        blacklist = [
            '[document]',
            'noscript',
            'header',
            'html',
            'meta',
            'head',
            'input',
            'script',
        ]

        output = []
        for t in text:
            # check only the parts of html we need
            if t.parent.name not in blacklist:
                # check if t has numbers in it
                res = re.findall(r'[0-9]+,[0-9]+', t)
                date = re.findall(r'([0-9]{2}.[0-9]{2}.[0-9]{4}) ([0-9]{2}:[0-9]{2})', t)
                if res != []:
                    output.append(float(res[0].replace(',', '.')))
                elif date != []:
                    date = " ".join(date[0])
                    date = self.timetounix(date)
                    output.append(date)
        # remove unused values
        output = output[5:]
        return output

    # Convert scraped time to unix epoch
    def timetounix(self, strtime: str):
        res = time.mktime(datetime.datetime.strptime(strtime, "%d.%m.%Y %H:%M").timetuple())
        return res

if __name__ == "__main__":
    db = DB(path=Path(__file__).parent.absolute(), db="vran.sqlite", table="vranov")
    s = Scraper(url="URL HERE")
    header = s.getHeader()
    html = s.getHTML(header, s.url)
    res = s.parseHTML(html)
    db.save(res)