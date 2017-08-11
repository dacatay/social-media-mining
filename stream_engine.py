from __future__ import print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
import sys
import os
from pymongo import MongoClient

import time


class settings():
    ACCESS_TOKEN = '1960625461-KC6CvEuArUo5uP96NDPPGcowofXyhHKwZTeSyCX'
    ACCESS_TOKEN_SECRET = 'AdCNRsUB8gF7tP51TKFcmLDklyPWCSyTA7znGK5suexwi'
    CONSUMER_KEY = 'MJVM1DljLzn4adO93ntVGNjCG'
    CONSUMER_SECRET = 'hXILKtYkaTlqhRprlz0OErzK32A1udTY00UNhlTpUpDeYX2aq8'
    SEARCH_WORDS = ['salesforce', 'javascript', 'python']
    DATA_STORAGE = 'json'

    MONGO_HOST = '2oiewhfew'
    MONGO_USER = ''
    MONGO_ACCESS = ''
    MONGO_PASS = ''
    MONGO_DB = 'twitterdb'
    MONGO_COLLECTION = 'twitter_stream_data'

    MYSQL_HOST = 'oiufwefew'
    MYSQL_DB = ''
    MYSQL_TABLE = 'twitter_stream_data'


class TwitterStreamListener(StreamListener):

    def on_connect(self):
        print("You are now connected to the streaming API.")


    def on_data(self, data):

        if settings.DATA_STORAGE == 'txt':
            try:
                print(data)
                with open('data.txt', 'a') as file:
                    file.write(data)
                    file.write('\n')
                file.close()
                return True
            except BaseException or Exception as e:
                print('Failed on Data', str(e))
                time.sleep(5)
                return True

        if settings.DATA_STORAGE == 'json' or '':
            try:
                print(data)
                with open('data.json', 'a') as file:
                    file.write(data)
                file.close()
                return True
            except BaseException or Exception as e:
                print('Failed on Data', str(e))
                time.sleep(5)
                return True

        if settings.DATA_STORAGE == 'mongodb':
            try:
                client = MongoClient(settings.MONGO_HOST)
                # create and use twitterdb as data base
                db = client.twitterdb
                data_json = json.loads(data)
                db.twitter_stream_data.insert(data_json)
                return True
            except BaseException or Exception as e:
                print('Failed on Data', str(e))
                time.sleep(5)
                return True

        if settings.DATA_STORAGE == 'mysql':
            try:
                client = MongoClient(settings.MONGO_HOST)
                # create and use twitterdb as data base
                db = client.twitterdb
                data_json = json.loads(data)
                db.twitter_stream_data.insert(data_json)
                return True
            except BaseException or Exception as e:
                print('Failed on Data', str(e))
                time.sleep(5)
                return True


    def on_error(self, status):
        print(status)
        return True


def main():
    auth = OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
    auth.set_access_token(settings.ACCESS_TOKEN, settings.ACCESS_TOKEN_SECRET)
    listener = TwitterStreamListener(api=tweepy.API(wait_on_rate_limit=True))
    stream = Stream(auth=auth, listener=listener)
    stream.filter(track=settings.SEARCH_WORDS)


if __name__ == 'main':
    if __name__ == '__main__':
        try:
            main()
        except KeyboardInterrupt:
            print('Interrupted')
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
