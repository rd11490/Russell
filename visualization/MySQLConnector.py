import mysql.connector
import json
import pandas as pd


class MySQLConnector:
    def __init__(self):
        creds_file = open("cred/MySqlCred.json", "r")
        self.__creds = json.loads(creds_file.read())

    def __username(self):
        return self.__creds['MySQL']['Username']

    def __password(self):
        return self.__creds['MySQL']['Password']

    def runQuery(self, query):
        """
        Takes in a query string and outputs a pandas dataframe of the results
        :param query: String Query
        :return: Pandas dataframe
        """
        cnx = mysql.connector.connect(database='nba', user=self.__username(), password=self.__password())
        df = pd.read_sql(query, con=cnx)
        return df
