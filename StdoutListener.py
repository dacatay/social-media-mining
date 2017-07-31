from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time

from settings import *


class StdoutListener(StreamLsitener):

    def on_data(self, data):
        try:
            print(data)
            savefile = open('link', 'a')
            savefile.write(data)
            savefile.write('\n')
            savefile.close()
            return True
        except BaseExcepation as e:
            print('Failed on Data', str(e))
            time.sleep(5)

    def on_error(self,status):
        print(status)


if __name__ == 'main':
    listener = StdoutListener()
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = Stream(auth, listener)
    stream.filter(track=['salesforce','javascript','python'])