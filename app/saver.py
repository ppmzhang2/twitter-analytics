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
    SEED_USERS = [
        User(id=977458805658173440,
             screen_name='Silme29051012',
             name='Silme',
             description='',
             created_at='Sat Mar 24 08:15:14 +0000 2018',
             followers_count=3296,
             friends_count=11),
        User(id=390178855,
             screen_name='chinarealvoice',
             name='中国之音\U0001f1e8\U0001f1f3中国自己的声音\U0001f1e8\U0001f1f3',
             description=
             'spread positive energy, let the world understand China! '
             '\n再艰难，爱不会离开！此刻我们都是武汉人！'
             '\n万众一心，众志成城，就没有中国人民跨不过去的坎！'
             '\n武汉加油！中国\U0001f1e8\U0001f1f3加油！',
             created_at='Thu Oct 13 15:50:04 +0000 2011',
             followers_count=7407,
             friends_count=475),
        User(id=972969342437507072,
             screen_name='wb4966',
             name='北京摄影老顽童\U0001f1e8\U0001f1f3',
             description='两制可以谈，一国是关键！'
             '中华人民共和国的领土上决不能容纳一个汉奸！我挺近平！',
             created_at='Sun Mar 11 22:55:42 +0000 2018',
             followers_count=3839,
             friends_count=106),
        User(id=1154405239052627968,
             screen_name='zifeiyu_1003_2',
             name='子非鱼_II世',
             description='“子非鱼，焉知鱼之乐?” …“子非我，安知我不知鱼之乐?” '
             '…… 尔曹身与名俱灭，不废江河万古流！反贼 公知滚远点！',
             created_at='Thu Jul 25 14:57:11 +0000 2019',
             followers_count=2053,
             friends_count=395),
        User(id=577007325,
             screen_name='iutiku',
             name='徐嘉苧',
             description='生如夏花，逝若流沙。人身一苦器，生死两刹那！',
             created_at='Fri May 11 07:18:27 +0000 2012',
             followers_count=9710,
             friends_count=177),
        User(id=1174230449348055040,
             screen_name='Boyanxiejun',
             name='五毛外宣部副部长\U0001f1e8\U0001f1f3',
             description='敲响世界法西斯的丧钟，'
             '让挑事好战的人都变盒子，'
             '收集友军尸体，'
             '欢迎各位把尸体发给我，',
             created_at='Wed Sep 18 07:55:44 +0000 2019',
             followers_count=2503,
             friends_count=2391),
        User(id=2924811511,
             screen_name='XijinLi',
             name='XIJIN LI',
             description='看见。思考。记录。',
             created_at='Tue Dec 09 23:33:33 +0000 2014',
             followers_count=7930,
             friends_count=2446),
        User(id=1153839774567825410,
             screen_name='Bridge__z',
             name='橋',
             description='予人玫瑰 手有餘香',
             created_at='Wed Jul 24 01:30:14 +0000 2019',
             followers_count=3315,
             friends_count=132)
    ]

    def reset(self):
        self.dao.reset_db()
        tweeter_ids = self.dao.bulk_save_tweeter(self.SEED_USERS)
        self.dao.bulk_save_wumao(list(tweeter_ids), new=True)

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

    def enlist_wumao(self) -> int:
        """recursively save qualified tweeters into table 'wumao'

        :return: loop count
        """
        def test(tweeter_id: int, d: Dao):
            """check whether or not a tweeter is wumao

            :param tweeter_id:
            :param d: instance of Dao
            :return:
            """
            score = 1.5 * d.follower_count(tweeter_id, d.all_wumao_tweeter_id(
            )) + 1.0 * d.friend_count(tweeter_id, d.all_wumao_tweeter_id())
            if score >= 4:
                return True
            else:
                return False

        n = 0
        while True:
            candidate_tweeter_ids = self.dao.all_tweeter_id(
            ) - self.dao.all_wumao_tweeter_id()
            new_wumao_tweeter_ids = [
                i for i in candidate_tweeter_ids if test(i, self.dao)
            ]
            if not new_wumao_tweeter_ids:
                break
            else:
                self.dao.bulk_save_wumao(new_wumao_tweeter_ids, new=False)
            n += 1
        return n

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
