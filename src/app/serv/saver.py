"""record saver"""
import logging
import math
import os
import shutil
from datetime import date
from datetime import datetime
from functools import wraps
from time import sleep
from typing import NoReturn
from typing import Optional

import requests
import twitter.error
from twitter.models import User

from ..models.dao import Dao
from ..singleton import SingletonMeta
from .tweet import Tweet

LOGGER = logging.getLogger(__name__)

__all__ = ['Saver']


def _sleep(fn):

    @wraps(fn)
    def helper(*args, **kwargs):
        while True:
            try:
                res = fn(*args, **kwargs)
                break
            except (twitter.error.TwitterError,
                    requests.exceptions.ConnectionError) as e:
                if isinstance(e, requests.exceptions.ConnectionError):
                    # sleep 5 min if connection reset by peer
                    LOGGER.info("connection error, sleep...")
                    sleep(300)
                elif e.message == [{
                        'message': 'Rate limit exceeded',
                        'code': 88
                }]:
                    # sleep 6 min if exceeds limit
                    LOGGER.info("exceeds rate limit, sleep...")
                    sleep(360)
                else:
                    # else raise error
                    raise e
        return res

    return helper


class Saver(metaclass=SingletonMeta):
    """dao / tweet wrapper to save records"""
    __slots__ = ['dao', 'tweet']

    PAGE_COUNT = 200
    CONSUMER_KEY = ''
    CONSUMER_SECRET = ''
    ACCESS_TOKEN = ''
    ACCESS_TOKEN_SECRET = ''
    PROJECT_DIR = ''
    _APP_DB = 'app.db'
    _BAK_DB = 'app.db.bak'

    def __init__(self):
        self.dao = Dao(self.app_db())
        self.tweet = Tweet(
            self.CONSUMER_KEY,
            self.CONSUMER_SECRET,
            self.ACCESS_TOKEN,
            self.ACCESS_TOKEN_SECRET,
        )

    @classmethod
    def app_db(cls):
        """sqlite database path"""
        return os.path.join(cls.PROJECT_DIR, cls._APP_DB)

    @classmethod
    def bak_db(cls):
        """backup database path"""
        return os.path.join(cls.PROJECT_DIR, cls._BAK_DB)

    @classmethod
    def update_params(
        cls,
        consumer_key: str,
        consumer_secret: str,
        access_token: str,
        access_token_secret: str,
        project_path: str,
    ) -> NoReturn:
        """update token and secret"""
        cls.CONSUMER_KEY = consumer_key
        cls.CONSUMER_SECRET = consumer_secret
        cls.ACCESS_TOKEN = access_token
        cls.ACCESS_TOKEN_SECRET = access_token_secret
        cls.PROJECT_DIR = project_path

    def reset(self):
        """reset state"""
        shutil.rmtree(self.app_db(), ignore_errors=True)
        self.dao.reset_db()

    def seeds(self, *args: int):
        """add seeds"""
        seed_users = [self.tweet.get_user(i) for i in args]
        tweeter_ids = self.dao.bulk_save_tweeter(seed_users)
        self.dao.bulk_save_wumao(list(tweeter_ids), new=True)

    def export(self, csv_path: str):
        """export to csv"""
        self.dao.wumao_to_csv(csv_path)

    @staticmethod
    def _is_potential_wumao(user: User) -> bool:
        """check if it is a newly registered wumao twitter account

        :param user: a twitter.models.User object
        :return:
        """

        def _parse_date(timestamp):
            """parse tweet "created_at" timestamp string

            :param timestamp: "created_at" format string
            :return: datetime.date object
            """
            ts = datetime.strptime(timestamp, '%a %b %d %H:%M:%S +0000 %Y')
            return ts.date()

        if user.protected:
            return False
        if _parse_date(user.created_at) < date(2011, 1, 1):
            return False
        if user.followers_count > 5000:
            return False
        return True

    @staticmethod
    def _next_func_name(fn_name: str = None) -> Optional[str]:
        """get the next function name

        :param fn_name:
        :return:
        """
        if not fn_name:
            return 'get_following_paged'
        if fn_name == 'get_following_paged':
            return 'get_followers_paged'
        if fn_name == 'get_followers_paged':
            return None
        raise ValueError('invalid function name')

    def _search_params(self, tweeter_id):
        """return parameters for twitter friends & followers searching

        :param tweeter_id: 'tweeter' primary key
        :return: tuple of user_id, cursor, paged function, is followers flag
        """

        def _is_follower(fn_name: str):
            """check if the function returns a follower list

            :param fn_name: twitter paged function name
            :return:
            """
            if fn_name == 'get_followers_paged':
                return True
            return False

        # raise exception if ID not exist
        self.dao.constrain_tweeter_exist(tweeter_id)
        user_id = self.dao.lookup_tweeter(tweeter_id).user_id
        last_search = self.dao.lookup_track(tweeter_id)
        if not last_search:
            cursor = -1
            func_name = self._next_func_name()
        else:
            cursor = last_search.cursor
            func_name = last_search.method
        is_followers = _is_follower(func_name)
        return user_id, cursor, getattr(Tweet, func_name), is_followers

    def _save_db(self, tweeter_id: int, seq: list, followers: bool):
        wumaos = [u for u in seq if self._is_potential_wumao(u)]
        # save to 'tweeter'
        wumao_tweeter_ids = self.dao.bulk_save_tweeter(wumaos, return_all=True)
        LOGGER.info(f"#Wumao: {len(wumaos)}")
        # save to friendship
        if followers:
            self.dao.bulk_attract(tweeter_id, wumao_tweeter_ids)
        else:
            self.dao.bulk_follow(tweeter_id, wumao_tweeter_ids)

    @_sleep
    def _add_friendship(self, tweeter_id: int):
        """add friends & followers of a twitter account in 'tweeter' table

        :param tweeter_id:
        :return:
        """
        user_id, cursor, paged_method, is_followers = self._search_params(
            tweeter_id)
        LOGGER.info(
            f"start saving {paged_method.__name__} from cursor {cursor}")
        next_cursor, _, seq = paged_method(
            self.tweet,
            user_id=user_id,
            cursor=cursor,
            count=self.PAGE_COUNT,
        )

        self._save_db(tweeter_id, seq, is_followers)

        # all finished for one wumao account
        #   1. set is_new = 0 in table 'wumao'
        #   2. delete record in 'track'
        if next_cursor == 0 and self._next_func_name(
                paged_method.__name__) is None:
            self.dao.upsert_wumao(tweeter_id, False)
            self.dao.delete_track(tweeter_id)
            LOGGER.info("friendship saving for {tweeter_id} completed")
        # search with next paged function
        if next_cursor == 0:
            next_func_name = self._next_func_name(paged_method.__name__)
            real_next_cursor = -1
        # search with the same function but next cursor
        else:
            next_func_name = paged_method.__name__
            real_next_cursor = next_cursor

        self.dao.upsert_track(tweeter_id, next_func_name, real_next_cursor)
        return self._add_friendship(tweeter_id)

    def add_friendship(self) -> NoReturn:
        """add friendship"""
        while True:
            last_search = self.dao.any_track()
            if last_search is not None:
                self._add_friendship(last_search.tweeter_id)
            else:
                wumao = self.dao.any_wumao(True)
                if wumao is None:
                    break
                LOGGER.info(f'searching account: {wumao.tweeter_id}')
                self._add_friendship(wumao.tweeter_id)
        LOGGER.info('all friendship of new wumaos has been added')

    def enlist_wumao(self, lower_bound: float = 0) -> int:
        """save to wumao list tweeters with the highest wumao score, if the
        score is higher than or equal to the provided lower bound, and refresh
        wumao weight using their internal connection score

        :param lower_bound: lower bound of the highest score, default 0
        :return: current highest wumao score, -1 if no candidate selected
        """
        score_card = self.dao.score()

        if not score_card:
            return -1

        # relax a bit criteria by using floor
        max_score = math.floor(max(score_card, key=lambda x: x.score).score)

        if max_score >= lower_bound:
            new_wumao_tweeter_ids = [
                r.tweeter_id for r in score_card if r.score >= max_score
            ]
            LOGGER.info(f'tweeter IDs to save: {new_wumao_tweeter_ids}')
            self.dao.bulk_save_wumao(new_wumao_tweeter_ids, new=True)
            # refresh weight after adding new wumaos
            self.dao.refresh_wumao_score()
        return max_score

    def search(self) -> NoReturn:
        """wumao calculation and searching
        finish if no wumao is enlisted after an adding friendship process

        :local threshold:
        the threshold is assumed to always increase as new wuamos are
        continuously added; assigned to half of total #wumao

        :return:
        """
        while True:
            threshold = len(self.dao.all_wumao_tweeter_id()) / 2
            self.add_friendship()
            new_max_score = self.enlist_wumao(threshold)
            LOGGER.info(f'current maximum score: {threshold}')
            if new_max_score < threshold:
                break
        LOGGER.info(
            f'all wumaos are found, job done! last max score: {threshold}')
