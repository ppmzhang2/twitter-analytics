import math
import shutil
from datetime import date, datetime
from functools import wraps
from time import sleep
from typing import Optional

import requests
import twitter.error
from twitter.models import User

from app.models.dao import Dao
from app.tweet import Tweet
from config import Config

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
                    print("connection error, sleep...")
                    sleep(300)
                elif e.message == [{
                        'message': 'Rate limit exceeded',
                        'code': 88
                }]:
                    # sleep 6 min if exceeds limit
                    print('backup DB when exceeds limit')
                    shutil.copyfile(Config.APP_DB, Config.BAK_DB)
                    print("exceeds rate limit, sleep...")
                    sleep(360)
                else:
                    # else raise error
                    raise e
        return res

    return helper


class SingletonMeta(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class Saver(metaclass=SingletonMeta):
    __slots__ = ['dao', 'tweet']

    def __init__(self):
        self.dao = Dao()
        self.tweet = Tweet()

    PAGE_COUNT = 200

    def reset(self):
        shutil.rmtree(Config.APP_DB, ignore_errors=True)
        self.dao.reset_db()

    def seeds(self, *args: int):
        seed_users = [self.tweet.get_user(i) for i in args]
        tweeter_ids = self.dao.bulk_save_tweeter(seed_users)
        self.dao.bulk_save_wumao(list(tweeter_ids), new=True)

    def export(self):
        self.dao.wumao_to_csv()

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
        elif _parse_date(user.created_at) < date(2011, 1, 1):
            return False
        elif user.followers_count > 5000:
            return False
        else:
            return True

    @staticmethod
    def _next_func_name(fn_name: str = None) -> Optional[str]:
        """get the next function name

        :param fn_name:
        :return:
        """
        if not fn_name:
            return 'get_following_paged'
        elif fn_name == 'get_following_paged':
            return 'get_followers_paged'
        elif fn_name == 'get_followers_paged':
            return None
        else:
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
            else:
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
        print("#Wumao:", len(wumaos))
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
        print("start saving {0} from cursor {1}".format(
            paged_method.__name__, cursor))
        next_cursor, old_cursor, seq = paged_method(self.tweet,
                                                    user_id=user_id,
                                                    cursor=cursor,
                                                    count=self.PAGE_COUNT)

        self._save_db(tweeter_id, seq, is_followers)

        # all finished for one wumao account
        #   1. set is_new = 0 in table 'wumao'
        #   2. delete record in 'track'
        if next_cursor == 0 and self._next_func_name(
                paged_method.__name__) is None:
            self.dao.upsert_wumao(tweeter_id, False)
            self.dao.delete_track(tweeter_id)
            print("friendship saving for {} completed".format(tweeter_id))
            return
        # search with next paged function
        elif next_cursor == 0:
            next_func_name = self._next_func_name(paged_method.__name__)
            real_next_cursor = -1
        # search with the same function but next cursor
        else:
            next_func_name = paged_method.__name__
            real_next_cursor = next_cursor

        self.dao.upsert_track(tweeter_id, next_func_name, real_next_cursor)
        return self._add_friendship(tweeter_id)

    def add_friendship(self):
        while True:
            last_search = self.dao.any_track()
            if last_search is not None:
                self._add_friendship(last_search.tweeter_id)
            else:
                wumao = self.dao.any_wumao(True)
                if wumao is None:
                    break
                else:
                    print('searching account: ', wumao.tweeter_id)
                    self._add_friendship(wumao.tweeter_id)
        print('all friendship of new wumaos has been added')
        return

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
            print('tweeter IDs to save: {}'.format(new_wumao_tweeter_ids))
            self.dao.bulk_save_wumao(new_wumao_tweeter_ids, new=True)
            # refresh weight after adding new wumaos
            self.dao.refresh_wumao_score()
        return max_score

    def automaton(self):
        """full-auto wumao searching
        finish if no wumao is enlisted after an adding friendship process

        :return:
        """
        while True:
            self.add_friendship()
            n = self.enlist_wumao()
            if n == 0:
                break
        print('job done')
        return
