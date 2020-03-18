import datetime
import time
from typing import Tuple

import twitter

from config import Config

__all__ = ['Tweet']


class SingletonMeta(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class Tweet(metaclass=SingletonMeta):
    __slots__ = ['api']

    def __init__(self):
        self.api = twitter.Api(consumer_key=Config.CONSUMER_KEY,
                               consumer_secret=Config.CONSUMER_SECRET,
                               access_token_key=Config.ACCESS_TOKEN,
                               access_token_secret=Config.ACCESS_TOKEN_SECRET)

    @staticmethod
    def parse_date(timestamp):
        """parse tweet "created_at" timestamp string

        :param timestamp: "created_at" format string
        :return: datetime.date object
        """
        ts = time.strptime(timestamp, '%a %b %d %H:%M:%S +0000 %Y')
        return datetime.date(ts.tm_year, ts.tm_mon, ts.tm_mday)

    @staticmethod
    def is_junior_wumao(user: twitter.models.User) -> bool:
        """check if it is a newly registered wumao twitter account

        :param user: a twitter.models.User object
        :return:
        """
        if user.protected:
            return False
        elif Tweet.parse_date(user.created_at) < datetime.date(2020, 1, 1):
            return False
        elif user.followers_count > 30:
            return False
        elif user.statuses_count + user.favourites_count < 500:
            return False
        else:
            return True

    def get_followers_paged(
            self,
            user_id,
            cursor=-1,
            count=200,
            skip_status=True,
            include_user_entities=False) -> Tuple[int, int, list]:
        return self.api.GetFollowersPaged(
            user_id=user_id,
            cursor=cursor,
            count=count,
            skip_status=skip_status,
            include_user_entities=include_user_entities)

    def get_following_paged(
            self,
            user_id,
            cursor=-1,
            count=200,
            skip_status=True,
            include_user_entities=False) -> Tuple[int, int, list]:
        return self.api.GetFriendsPaged(
            user_id=user_id,
            cursor=cursor,
            count=count,
            skip_status=skip_status,
            include_user_entities=include_user_entities)

    def get_followers(self,
                      user_id,
                      skip_status=True,
                      include_user_entities=False) -> list:
        """get followers via Tweet.get_followers_paged, maximum 200

        :param user_id:
        :param skip_status:
        :param include_user_entities:
        :return:
        """
        return self.get_followers_paged(user_id, -1, 200, skip_status,
                                        include_user_entities)[2]

    def get_following(self,
                      user_id,
                      skip_status=True,
                      include_user_entities=False) -> list:
        """get followings via Tweet.get_following_paged, maximum 200

        :param user_id:
        :param skip_status:
        :param include_user_entities:
        :return:
        """
        return self.get_following_paged(user_id, -1, 200, skip_status,
                                        include_user_entities)[2]
