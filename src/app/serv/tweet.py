"""tweet logic"""
import datetime
import time
from functools import wraps
from typing import Any
from typing import List
from typing import Optional
from typing import Tuple

import twitter
from twitter.models import User

from ..singleton import SingletonMeta

__all__ = ['Tweet']


def _catcher(default: Any):
    """decorator to catch TwitterError (e.g. unauthorized due to account
    suspension)

    :param default: default value to return when error is caught
    :return:
    """

    def dec(fn):

        @wraps(fn)
        def helper(*args, **kwargs):
            try:
                res = fn(*args, **kwargs)
            except twitter.error.TwitterError as e:
                if e.message == 'Not authorized.':
                    return default
                raise e

            return res

        return helper

    return dec


class Tweet(metaclass=SingletonMeta):
    """tweet API class"""
    __slots__ = ['api']

    def __init__(
        self,
        consumer_key: str,
        consumer_secret: str,
        access_token: str,
        access_token_secret: str,
    ):
        self.api = twitter.Api(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token_key=access_token,
            access_token_secret=access_token_secret,
        )

    @staticmethod
    def parse_date(timestamp: str) -> datetime.date:
        """parse tweet "created_at" timestamp string

        :param timestamp: "created_at" format string
        :return: datetime.date object
        """
        ts = time.strptime(timestamp, '%a %b %d %H:%M:%S +0000 %Y')
        return datetime.date(ts.tm_year, ts.tm_mon, ts.tm_mday)

    @_catcher((0, -1, []))
    def get_followers_paged(
        self,
        user_id: int,
        cursor: int = -1,
        count: int = 200,
        skip_status: bool = True,
        include_user_entities: bool = False,
    ) -> Tuple[int, int, List[User]]:
        """get followers paged"""
        return self.api.GetFollowersPaged(
            user_id=user_id,
            cursor=cursor,
            count=count,
            skip_status=skip_status,
            include_user_entities=include_user_entities)

    @_catcher((0, -1, []))
    def get_following_paged(
        self,
        user_id: int,
        cursor: int = -1,
        count: int = 200,
        skip_status: bool = True,
        include_user_entities: bool = False,
    ) -> Tuple[int, int, List[User]]:
        """get following paged"""
        return self.api.GetFriendsPaged(
            user_id=user_id,
            cursor=cursor,
            count=count,
            skip_status=skip_status,
            include_user_entities=include_user_entities)

    @_catcher([])
    def get_followers(self,
                      user_id: int,
                      skip_status: bool = True,
                      include_user_entities: bool = False) -> List[User]:
        """get followers via Tweet.get_followers_paged, maximum 200

        :param user_id:
        :param skip_status:
        :param include_user_entities:
        :return:
        """
        return self.get_followers_paged(user_id, -1, 200, skip_status,
                                        include_user_entities)[2]

    @_catcher([])
    def get_following(self,
                      user_id: int,
                      skip_status=True,
                      include_user_entities=False) -> List[User]:
        """get followings via Tweet.get_following_paged, maximum 200

        :param user_id:
        :param skip_status:
        :param include_user_entities:
        :return:
        """
        return self.get_following_paged(user_id, -1, 200, skip_status,
                                        include_user_entities)[2]

    @_catcher(None)
    def get_user(self, user_id: int) -> Optional[User]:
        """add user"""
        return self.api.GetUser(user_id=user_id)
