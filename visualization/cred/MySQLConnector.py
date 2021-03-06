import json

import mysql.connector
import pandas as pd
import MySqlDatabases.NBADatabase
from sqlalchemy import create_engine
import os


class MySQLConnector:
    def __init__(self):

        creds_file = open(os.path.join(os.path.dirname(__file__), './MySqlCred.json'), "r")
        self.__creds = json.loads(creds_file.read())

    def __username(self):
        return self.__creds['MySQL']['Username']

    def __password(self):
        return self.__creds['MySQL']['Password']

    def __build_engine(self, database):
        ssl_args = {'ssl_disabled': True}
        return create_engine(
            'mysql+mysqlconnector://{0}:{1}@localhost:3306/{2}'.format(self.__username(), self.__password(), database), connect_args=ssl_args)

    def runQuery(self, query, database=MySqlDatabases.NBADatabase.NAME):
        """
        Takes in a query string and outputs a pandas dataframe of the results
        :param query: String Query
        :param database: String database name
        :return: Pandas dataframe
        """
        cnx = mysql.connector.connect(database=database, user=self.__username(), password=self.__password(), ssl_disabled=True)
        df = pd.read_sql(query, con=cnx)
        return df

    def write(self, df, table, database=MySqlDatabases.NBADatabase.NAME, dtype=None):
        """
        Takes in a query string and outputs a pandas dataframe of the results
        :param database:
        :param query: String Query
        """
        # cnx = mysql.connector.connect(database=database, user=self.__username(), password=self.__password())
        # username:password@host:port/database
        engine = self.__build_engine(database)
        df.to_sql(name=table, con=engine, if_exists='append', index=False, dtype=dtype)

    def drop_table(self, table, database):
        engine = self.__build_engine(database)
        engine.execute("DROP TABLE IF EXISTS {}".format(table))


    def truncate_table(self, table, database, where = "1 = 1"):
        engine = self.__build_engine(database)
        engine.execute("DELETE FROM {0} WHERE {1}".format(table, where))