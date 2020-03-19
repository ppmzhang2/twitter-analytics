from datetime import date
from functools import wraps
from time import sleep

import requests
import twitter

from app.models.dao import Dao
from app.models.tables import Tweeter, BaseTweeter
from app.tweet import Tweet

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
        self.dao = Dao(new=False)
        self.tweet = Tweet()

    @staticmethod
    def _is_junior_wumao(user: twitter.models.User) -> bool:
        """check if it is a newly registered wumao twitter account

        :param user: a twitter.models.User object
        :return:
        """
        if user.protected:
            return False
        elif Tweet.parse_date(user.created_at) < date(2020, 1, 1):
            return False
        elif user.followers_count > 30:
            return False
        elif user.statuses_count + user.favourites_count < 500:
            return False
        else:
            return True

    @staticmethod
    def _user_to_tweeter(user: twitter.models.User):
        """convert a twitter.User instance to a Tweeter ORM object

        :param user: a twitter.User instance
        :return: a Tweeter object
        """
        return Tweeter(user.id, user.screen_name, user.name,
                       Tweet.parse_date(user.created_at), user.followers_count,
                       user.friends_count)

    @staticmethod
    def _user_to_base_tweeter(user: twitter.models.User):
        """convert a twitter.User instance to a BaseTweeter ORM object

        :param user: a twitter.User instance
        :return: a Tweeter object
        """
        return BaseTweeter(user.id)

    @_sleep
    def init_wumao(self, user_id: int):
        """save potential wumao users by searching a user's followers

        :param user_id: twitter user whose followers will be searched
        :return:
        """
        # use maximum count per page
        count = 200
        paged_method = self.tweet.get_followers_paged
        last_search = self.dao.lookup_track(user_id, paged_method.__name__)
        if not last_search:
            cursor = -1
        else:
            cursor = last_search.cursor
        print("start saving wumao from cursor:", cursor)
        next_cursor, old_cursor, seq = paged_method(user_id=user_id,
                                                    cursor=cursor,
                                                    count=count)
        print("#seq:", len(seq))
        wumaos = [u for u in seq if self._is_junior_wumao(u)]
        new_wumaos = [
            u for u in wumaos if self.dao.lookup_tweeter_user_id(u.id) is None
        ]
        print("#Wumao:", len(wumaos))
        print("#New Wumao:", len(new_wumaos))

        if new_wumaos:
            tweeter_wumao = [Saver._user_to_tweeter(u) for u in new_wumaos]
            base_tweeter_wumao = [
                Saver._user_to_base_tweeter(u) for u in new_wumaos
            ]
            self.dao.bulk_save(tweeter_wumao)
            self.dao.bulk_save(base_tweeter_wumao)

        if next_cursor == 0:
            print("wumao saving completed")
            return
        else:
            self.dao.update_track(user_id, paged_method.__name__, next_cursor)
            return self.init_wumao(user_id)

    @_sleep
    def validate_wumao(self):
        """validate wumao tweeter by checking if its followers or friends are
        in the initial wumao table

        :return:
        """
        zero_user = self.dao.first_base_tweeter()
        if zero_user is None:
            print("all wumaos are validated")
            return
        else:
            user_id = zero_user.user_id
            print("potential wumao:", user_id)
            related = [
                u for u in self.tweet.get_following(user_id=user_id)
            ] + [u for u in self.tweet.get_followers(user_id=user_id)]
            wumaos = [u for u in related if self._is_junior_wumao(u)]
            unique_wumaos = [
                u for u in wumaos
                if self.dao.lookup_tweeter_user_id(u.id) is not None
            ]
            if not unique_wumaos:
                print("NO wumao")
                self.dao.delete_tweeter_user_id(user_id)
                self.dao.delete_base_tweeter_user_id(user_id)
            else:
                print("confirmed wumao")
                self.dao.delete_base_tweeter_user_id(user_id)
            return self.validate_wumao()
