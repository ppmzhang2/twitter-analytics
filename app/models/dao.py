from datetime import datetime
from functools import wraps
from typing import List, Optional, Iterable, Set

import twitter
from sqlalchemy import create_engine, or_, func
from sqlalchemy.orm import sessionmaker, aliased

from app.models.base import Base
from app.models.tables import Tweeter, Friendship, Track, Wumao
from config import Config

__all__ = ['Dao']


def session_factory(echo: bool):
    engine = create_engine('sqlite:///{0}'.format(Config.APP_DB), echo=echo)
    _SessionFactory = sessionmaker(bind=engine)
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

    def __init__(self, echo=False):
        self.session = session_factory(echo)

    @staticmethod
    def _is_new(flag: bool):
        return {True: 1, False: 0}[flag]

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
        return Tweeter(user.id, user.screen_name, user.name, user.description,
                       Dao._parse_date(user.created_at), user.followers_count,
                       user.friends_count)

    def _delete_tweeter_cascade(self, tweeter_id: int) -> int:
        self.session.query(Friendship).filter(
            or_(Friendship.author_id == tweeter_id,
                Friendship.follower_id == tweeter_id)).delete()
        self.session.query(Wumao).filter(
            Wumao.tweeter_id == tweeter_id).delete()
        self.session.query(Track).filter(
            Track.tweeter_id == tweeter_id).delete()

    def constrain_tweeter_exist(self, tweeter_id: int) -> None:
        """check provided primary key of table 'tweeter', and raise value error
        if the PK_ID does NOT exist

        :param tweeter_id:
        :return:
        """
        if self.lookup_tweeter(tweeter_id) is None:
            raise ValueError('PK ID provided does NOT Exist!')

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

    def bulk_save_tweeter(self,
                          users: List[twitter.models.User],
                          return_all: bool = False) -> Set[int]:
        """bulk save on table 'tweeter'
        refer to dao.bulk_save

        :param users:
        :param return_all: whether return all primary keys of the input list,
        or only inserted ones, default False
        :return: a set of primary keys
        """
        existing_tweeter_ids = self.all_tweeter_id([u.id for u in users])
        if not existing_tweeter_ids:
            new_tweeters = set(self._twitter_user_mapper(u) for u in users)
        else:
            existing_tweeter_user_ids = set(
                self.lookup_tweeter(i).user_id for i in existing_tweeter_ids)
            new_tweeters = set(
                self._twitter_user_mapper(u) for u in users
                if u.id not in existing_tweeter_user_ids)

        self.bulk_save(new_tweeters)
        if return_all:
            return self.all_tweeter_id([u.id for u in users])
        else:
            return self.all_tweeter_id([u.user_id for u in new_tweeters])

    def lookup_tweeter(self, tweeter_id: int) -> Optional[Tweeter]:
        """get `Tweeter` instance by primary key

        :param tweeter_id: table 'tweeter' primary key
        :return: a `Tweeter` instance of None if no match
        """
        return self.session.query(Tweeter).filter(
            Tweeter.id == tweeter_id).first()

    def all_tweeter_id(self, user_ids: Optional[List[int]] = None) -> Set[int]:
        """get matched `Tweeter` primary keys by user_id list provided, or
        all PKIDs

        :param user_ids: user_id `list`
        :return: set of table 'tweeter' primary keys
        """
        qry = self.session.query(Tweeter.id)
        if user_ids is None:
            return set(t[0] for t in qry.all())
        else:
            return set(
                t[0] for t in qry.filter(Tweeter.user_id.in_(user_ids)).all())

    @_commit
    def delete_tweeter(self, tweeter_id: int) -> int:
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

    def score(self):
        """scoring a twitter account by measuring its wumao friends & followers
        count

        :return: list of 1. tweeter_id; 2. score
        """
        tweeter_ids = self.all_wumao_tweeter_id()
        a1 = aliased(Friendship)
        a2 = aliased(Friendship)
        q1 = self.session.query(
            a1.follower_id,
            func.count(a1.author_id).label('friend_count')).filter(
                a1.author_id.in_(tweeter_ids),
                a1.follower_id.notin_(tweeter_ids)).group_by(a1.follower_id)
        q2 = self.session.query(
            a2.author_id,
            func.count(a2.follower_id).label('follower_count')).filter(
                a2.follower_id.in_(tweeter_ids),
                a2.author_id.notin_(tweeter_ids)).group_by(a2.author_id)
        s1 = q1.subquery()
        s2 = q2.subquery()
        return self.session.query(
            s1.c.follower_id.label('tweeter_id'),
            (s1.c.friend_count + s2.c.follower_count).label('score')).join(
                s2, s1.c.follower_id == s2.c.author_id).all()

    @_commit
    def follow(self, tweeter_id: int, author_id: int) -> None:
        self.constrain_tweeter_exist(tweeter_id)
        self.constrain_tweeter_exist(author_id)
        if not self.is_following(tweeter_id, author_id):
            self.session.add(Friendship(author_id, tweeter_id))

    @_commit
    def un_follow(self, tweeter_id: int, author_id: int) -> None:
        self.constrain_tweeter_exist(tweeter_id)
        self.constrain_tweeter_exist(author_id)
        if self.is_following(tweeter_id, author_id):
            self.session.query(Friendship).filter(
                Friendship.author_id == author_id,
                Friendship.follower_id == tweeter_id).delete()

    def bulk_follow(self, tweeter_id: int, authors: List[int]) -> None:
        self.constrain_tweeter_exist(tweeter_id)
        [self.constrain_tweeter_exist(i) for i in authors]
        new_authors = [
            i for i in authors if i not in self.friends_id(tweeter_id)
        ]
        self.bulk_save((Friendship(i, tweeter_id) for i in new_authors))

    def bulk_attract(self, tweeter_id: int, followers: List[int]) -> None:
        self.constrain_tweeter_exist(tweeter_id)
        [self.constrain_tweeter_exist(i) for i in followers]
        new_followers = [
            i for i in followers if i not in self.followers_id(tweeter_id)
        ]
        self.bulk_save((Friendship(tweeter_id, i) for i in new_followers))

    def any_wumao(self, new: bool = False) -> Optional[Wumao]:
        is_new = self._is_new(new)
        return self.session.query(Wumao).filter(Wumao.is_new == is_new).first()

    def lookup_wumao(self, wumao_id: int) -> Optional[Wumao]:
        return self.session.query(Wumao).filter(Wumao.id == wumao_id).first()

    def bulk_save_wumao(self,
                        tweeter_ids: List[int],
                        new: bool = False,
                        return_all: bool = False) -> Set[int]:
        """bulk save on table 'wumao'
        refer to dao.bulk_save

        :param new:
        :param tweeter_ids:
        :param return_all: whether return all primary keys of the input list,
        or only inserted ones, default False
        :return: set of primary keys
        """
        [self.constrain_tweeter_exist(i) for i in tweeter_ids]
        is_new = self._is_new(new)
        existing_wumao_ids = self.all_wumao_id(tweeter_ids)
        if not existing_wumao_ids:
            new_wumaos = set(Wumao(i, is_new) for i in tweeter_ids)
        else:
            existing_wumao_tweeter_ids = set(
                self.lookup_wumao(i).tweeter_id for i in existing_wumao_ids)
            new_wumaos = set(
                Wumao(i, is_new) for i in tweeter_ids
                if i not in existing_wumao_tweeter_ids)
        self.bulk_save(new_wumaos)
        if return_all:
            return self.all_wumao_id(tweeter_ids)
        else:
            return self.all_wumao_id([n.tweeter_id for n in new_wumaos])

    def all_wumao_id(self,
                     tweeter_ids: Optional[List[int]] = None) -> Set[int]:
        """get all table 'wumao' primary keys, or matched by input 'tweeter' ID

        :param tweeter_ids: 'tweeter' ID list, optional
        :return: set of primary keys of table 'wumao'
        """
        qry = self.session.query(Wumao.id)
        if tweeter_ids is None:
            return set(t[0] for t in qry.all())
        else:
            return set(
                t[0]
                for t in qry.filter(Wumao.tweeter_id.in_(tweeter_ids)).all())

    def all_wumao_tweeter_id(self) -> Set[int]:
        return set(t[0] for t in self.session.query(Wumao.tweeter_id).all())

    @_commit
    def upsert_wumao(self, tweeter_id: int, new: bool):
        self.constrain_tweeter_exist(tweeter_id)
        is_new = self._is_new(new)
        qry = self.session.query(Wumao).filter(Wumao.tweeter_id == tweeter_id)
        if qry.first() is None:
            self.session.add(Wumao(tweeter_id, is_new))
        else:
            qry.update({Wumao.is_new: is_new})

    def any_track(self) -> Track:
        return self.session.query(Track).first()

    def lookup_track(self, tweeter_id: int) -> Track:
        return self.session.query(Track).filter(
            Track.tweeter_id == tweeter_id).first()

    @_commit
    def delete_track(self, tweeter_id: int) -> int:
        """delete from 'track' records by providing 'tweeter' ID
        search method

        :param tweeter_id: 'tweeter' primary key
        :return: deleted number of records
        """
        return self.session.query(Track).filter(
            Track.tweeter_id == tweeter_id).delete()

    @_commit
    def upsert_track(self, tweeter_id: int, method: str, cur: int) -> None:
        """update or insert 'track' with latest cursor

        :param tweeter_id: 'tweeter' primary key
        :param method: search function name
        :param cur: cursor number
        :return:
        """
        self.constrain_tweeter_exist(tweeter_id)
        qry = self.session.query(Track).filter(Track.tweeter_id == tweeter_id)
        if qry.first() is None:
            self.session.add(Track(tweeter_id, method, cur))
        else:
            qry.update({Track.method: method, Track.cursor: cur})
