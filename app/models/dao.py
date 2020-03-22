from datetime import datetime
from functools import wraps
from typing import List, Optional, Iterable

import twitter
from sqlalchemy import create_engine, or_, func
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.tables import Tweeter, Friendship, Track, Wumao
from config import Config

__all__ = ['Dao']

# Let's also configure it to echo everything it does to the screen.
engine = create_engine('sqlite:///{0}'.format(Config.APP_DB), echo=True)

# use session_factory() to get a new Session
_SessionFactory = sessionmaker(bind=engine)


def session_factory():
    Base.metadata.create_all(engine)
    return _SessionFactory()


def _commit(fn):
    @wraps(fn)
    def helper(*args, **kwargs):
        res = fn(*args, **kwargs)
        args[0].session.commit()
        return res

    return helper


class SingletonMeta(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class Dao(metaclass=SingletonMeta):
    __slots__ = ['session']

    def __init__(self):
        self.session = session_factory()

    @staticmethod
    def _parse_date(timestamp):
        """parse tweet "created_at" timestamp string

        :param timestamp: "created_at" format string
        :return: datetime.date object
        """
        ts = datetime.strptime(timestamp, '%a %b %d %H:%M:%S +0000 %Y')
        return ts.date()

    @staticmethod
    def _twitter_user_mapper(user: twitter.models.User):
        """convert a twitter.User instance to a Tweeter ORM object

        :param user: a twitter.User instance
        :return: a Tweeter object
        """
        return Tweeter(user.id, user.screen_name, user.name,
                       Dao._parse_date(user.created_at), user.followers_count,
                       user.friends_count)

    def _delete_tweeter_cascade(self, tweeter_id: int) -> int:
        self.session.query(Friendship).filter(
            or_(Friendship.author_id == tweeter_id,
                Friendship.follower_id == tweeter_id)).delete()
        self.session.query(Wumao).filter(
            Wumao.tweeter_id == tweeter_id).delete()

    @_commit
    def reset_db(self) -> None:
        self.session.query(Track).delete()
        self.session.query(Friendship).delete()
        self.session.query(Wumao).delete()
        self.session.query(Tweeter).delete()

    @_commit
    def bulk_save(self, objects: Iterable) -> None:
        """Perform a bulk save of the given sequence of objects

        :param objects: a sequence of mapped object instances
        :return:
        """
        self.session.bulk_save_objects(objects)

    def bulk_save_tweeter(self, users: List[twitter.models.User]) -> List[int]:
        """bulk save on table 'tweeter'
        refer to dao.bulk_save

        :param users:
        :return: sequence of inserted primary keys
        """
        tweeters = [self._twitter_user_mapper(u) for u in users]
        existing_user_ids = set(
            u.user_id
            for u in self.all_tweeter_user_id([r.user_id for r in tweeters]))
        new_tweeters = set(u for u in tweeters
                           if u.user_id not in existing_user_ids)
        self.bulk_save(new_tweeters)
        return [
            r.id for r in self.all_tweeter_user_id(
                [u.user_id for u in new_tweeters])
        ]

    def lookup_tweeter_id(self, pk_id: int) -> Optional[Tweeter]:
        """get `Tweeter` instance by primary key

        :param pk_id: table 'tweeter' primary key
        :return: a `Tweeter` instance of None if no match
        """
        return self.session.query(Tweeter).filter(Tweeter.id == pk_id).first()

    def lookup_tweeter_user_id(self, user_id: int) -> Optional[Tweeter]:
        """get `Tweeter` instance by column 'user_id'

        :param user_id: user_id of table 'tweeter'
        :return: a `Tweeter` instance of None if no match
        """
        return self.session.query(Tweeter).filter(
            Tweeter.user_id == user_id).first()

    def all_tweeter_user_id(self, user_ids: List[int]) -> List[Tweeter]:
        """get all matched `Tweeter` object by user_id

        :param user_ids: user_id `list`
        :return: list of `Tweeter` instances
        """
        return self.session.query(Tweeter).filter(
            Tweeter.user_id.in_(user_ids)).all()

    @_commit
    def delete_tweeter_id(self, tweeter_id: int) -> int:
        """delete from 'tweeter' by primary key, and delete from 'friendship'
        CASCADE

        :param tweeter_id: ID of table 'twitter'
        :return: deleted number of records
        """
        res = self.session.query(Tweeter).filter(
            Tweeter.id == tweeter_id).delete()
        self._delete_tweeter_cascade(tweeter_id)
        return res

    def is_following(self, tweeter_id: int, author_id: int) -> bool:
        friendship = self.session.query(Friendship).filter(
            Friendship.author_id == author_id,
            Friendship.follower_id == tweeter_id).first()
        if friendship is None:
            return False
        else:
            return True

    def followers_id(self, tweeter_id: int) -> List[int]:
        connections = self.session.query(Friendship).filter(
            Friendship.author_id == tweeter_id).all()
        return [u.follower_id for u in connections]

    def friends_id(self, tweeter_id: int) -> List[int]:
        connections = self.session.query(Friendship).filter(
            Friendship.follower_id == tweeter_id).all()
        return [u.author_id for u in connections]

    def follower_count(self,
                       tweeter_id: int,
                       follower_ids: Optional[List[int]] = None) -> int:
        qry = self.session.query(func.count(Friendship.follower_id))
        if not follower_ids:
            res = qry.filter(Friendship.author_id == tweeter_id).first()
        else:
            res = qry.filter(Friendship.author_id == tweeter_id,
                             Friendship.follower_id.in_(follower_ids)).first()
        if not res:
            return 0
        else:
            return res[0]

    def friend_count(self,
                     tweeter_id: int,
                     author_ids: Optional[List[int]] = None) -> int:
        qry = self.session.query(func.count(Friendship.author_id))
        if not author_ids:
            res = qry.filter(Friendship.follower_id == tweeter_id).first()
        else:
            res = qry.filter(Friendship.follower_id == tweeter_id,
                             Friendship.author_id.in_(author_ids)).first()
        if not res:
            return 0
        else:
            return res[0]

    @_commit
    def follow(self, tweeter_id: int, author_id: int) -> None:
        if not self.is_following(tweeter_id, author_id):
            self.session.add(Friendship(author_id, tweeter_id))

    @_commit
    def un_follow(self, tweeter_id: int, author_id: int) -> None:
        if self.is_following(tweeter_id, author_id):
            self.session.query(Friendship).filter(
                Friendship.author_id == author_id,
                Friendship.follower_id == tweeter_id).delete()

    def bulk_follow(self, tweeter_id: int, authors: List[int]) -> None:
        new_authors = [
            i for i in authors if i not in self.friends_id(tweeter_id)
        ]
        self.bulk_save((Friendship(i, tweeter_id) for i in new_authors))

    def bulk_attract(self, tweeter_id: int, followers: List[int]) -> None:
        new_followers = [
            i for i in followers if i not in self.followers_id(tweeter_id)
        ]
        self.bulk_save((Friendship(tweeter_id, i) for i in new_followers))

    def bulk_save_wumao(self, tweeter_ids: List[int]) -> List[int]:
        """bulk save on table 'wumao'
        refer to dao.bulk_save

        :param tweeter_ids:
        :return: sequence of inserted primary keys
        """
        existing_tweeter_ids = set(u.tweeter_id
                                   for u in self.all_wumao(tweeter_ids))
        new_wumaos = set(
            Wumao(i) for i in tweeter_ids if i not in existing_tweeter_ids)
        self.bulk_save(new_wumaos)
        return [
            w.id for w in self.all_wumao([n.tweeter_id for n in new_wumaos])
        ]

    def all_wumao(self,
                  tweeter_ids: Optional[List[int]] = None) -> List[Wumao]:
        """get all `Wumao` instances, or matched by input 'tweeter' ID

        :param tweeter_ids: 'tweeter' ID list, optional
        :return: list of `Wumao` instances
        """
        qry = self.session.query(Wumao)
        if tweeter_ids is None:
            return qry.all()
        else:
            return qry.filter(Wumao.tweeter_id.in_(tweeter_ids)).all()

    def lookup_track(self, user_id: int, method: str) -> Track:
        return self.session.query(Track).filter(
            Track.user_id == user_id, Track.method == method).first()

    @_commit
    def delete_track(self, user_id: int, method: str) -> int:
        """delete from 'track' records with specific screen name and paged
        search method

        :param user_id: twitter account ID
        :param method: search function name
        :return: deleted number of records
        """
        return self.session.query(Track).filter(
            Track.user_id == user_id, Track.method == method).delete()

    @_commit
    def add_track(self, user_id: int, method: str, cur: int) -> None:
        return self.session.add(Track(user_id, method, cur))

    @_commit
    def update_track(self, user_id: int, method: str, cur: int) -> None:
        """update 'track' with latest cursor

        :param user_id: twitter account ID
        :param method: search function name
        :param cur: cursor number
        :return:
        """
        qry = self.session.query(Track).filter(Track.user_id == user_id,
                                               Track.method == method)
        if qry.first() is None:
            self.session.add(Track(user_id, method, cur))
        else:
            qry.update({Track.cursor: cur})
