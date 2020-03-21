from functools import wraps
from typing import List, Optional, Iterable

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

    def bulk_save_tweeter(self, tweeters: List[Tweeter]) -> None:
        user_ids = [u.user_id for u in tweeters]
        existing_user_ids = set(u.user_id
                                for u in self.all_tweeter_user_id(user_ids))
        new_tweeters = (u for u in tweeters
                        if u.user_id not in existing_user_ids)
        return self.bulk_save(new_tweeters)

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

    def bulk_save_wumao(self, tweeter_ids: List[int]) -> None:
        existing_tweeter_ids = set(u.tweeter_id
                                   for u in self.all_wumao(tweeter_ids))
        new_wumaos = (Wumao(i) for i in tweeter_ids
                      if i not in existing_tweeter_ids)
        return self.bulk_save(new_wumaos)

    def all_wumao(self,
                  tweeter_ids: Optional[List[int]] = None) -> List[Wumao]:
        """get all `Wumao` instances, or matched by input 'tweeter' ID

        :param tweeter_ids: 'tweeter' ID list, optional
        :return: list of `Wumao` instances
        """
        qry = self.session.query(Wumao)
        if tweeter_ids:
            return qry.filter(Wumao.tweeter_id.in_(tweeter_ids)).all()
        else:
            return qry.all()

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
