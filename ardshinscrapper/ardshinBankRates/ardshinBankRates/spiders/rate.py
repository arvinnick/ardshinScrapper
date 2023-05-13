from datetime import datetime
from pathlib import Path

import scrapy
import sqlite3


def db_create_table(con: sqlite3.Connection):
    """
    gets a connection to a sqlite database
    :param con: connection to the database
    :return: courser object of the connection
    """
    con.execute("""
                        create table if not exists rates(   
                                    dtime TIMESTAMP,
                                    cur CHAR(3),
                                    buy FLOAT(3,2),
                                    sell FLOAT(3,2)
                                                                        )
                        """)
    con.commit()
    return con.cursor()


class RatesSpider(scrapy.Spider):
    name = "rate"

    css_selector_format = "#cash > table:nth-child(1) > tbody:nth-child(1) > tr > td:nth-child({}) > span:nth-child(" \
                          "1)::text"
    pair_css_selector = css_selector_format.format(1)
    buy_css_selector = css_selector_format.format(2)
    sell_css_selector = css_selector_format.format(3)

    start_urls = [
        'https://www.ardshinbank.am/en'
    ]

    def __init__(self, name=None, **kwargs):
        super().__init__(name)
        self.db_path = "/home/arvin/PycharmProjects/ardshinScrapper/ardshinscrapper/ardshinBankRates/db/db"

    def parse(self, response):
        """
        parser of the information. used internally
        """
        pairs = response.css(self.pair_css_selector).getall()
        buy_rates = response.css(self.buy_css_selector).getall()
        sell_rates = response.css(self.sell_css_selector).getall()
        for pair, buy_rate, sell_rate in zip(pairs, buy_rates, sell_rates):
            dtime = datetime.now()
            self.write_on_db([dtime, pair, buy_rate, sell_rate])

    def db_connect(self):
        """
        connects to the database and returns the connection object. used internally
        """

        con = sqlite3.connect(self.db_path)
        return con

    def write_a_row_on_db(self, data):
        """
        used internally to write a single row on the database
        :return: id of the written row
        """
        assert len(data) == 4
        assert isinstance(data, list)
        sql = """
                INSERT INTO rates(dtime, cur, buy, sell)
                VALUES (?,?,?,?)
              """
        con = self.db_connect()
        cur = con.cursor()
        cur.execute(sql, data)
        con.commit()

    def write_on_db(self, data):
        """
        this function writes data on the database. Used internally.
        """
        connection = self.db_connect()
        db_create_table(connection)
        self.write_a_row_on_db(data)
        connection.close()
