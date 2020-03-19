from functools import wraps

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models.base import Base
from app.models.tables import Tweeter, BaseTweeter, Track
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
        if cls._instance is None or kwargs['new'] is True:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class Dao(metaclass=SingletonMeta):
    __slots__ = ['session']

    def __init__(self, new: bool = False):
        if new:
            print("replacing old session")
        self.session = session_factory()

    @_commit
    def bulk_save(self, objects) -> None:
        """Perform a bulk save of the given sequence of objects

        :param objects: a sequence of mapped object instances
        :return:
        """
        self.session.bulk_save_objects(objects)

    def lookup_tweeter_user_id(self, user_id: int) -> Tweeter:
        return self.session.query(Tweeter).filter(
            Tweeter.user_id == user_id).first()

    @_commit
    def delete_tweeter_user_id(self, user_id: int) -> int:
        """delete from 'tweeter' records with specific user ID

        :param user_id: twitter account user ID
        :return: deleted number of records
        """
        return self.session.query(Tweeter).filter(
            Tweeter.user_id == user_id).delete()

    def first_base_tweeter(self) -> BaseTweeter:
        return self.session.query(BaseTweeter).first()

    @_commit
    def delete_base_tweeter_user_id(self, user_id: int) -> int:
        return self.session.query(BaseTweeter).filter(
            BaseTweeter.user_id == user_id).delete()

    def lookup_track(self, name: str, method: str) -> Track:
        return self.session.query(Track).filter(
            Track.screen_name == name, Track.method == method).first()

    @_commit
    def delete_track(self, name: str, method: str) -> int:
        """delete from 'track' records with specific screen name and paged
        search method

        :param name: twitter account screen name
        :param method: search function name
        :return: deleted number of records
        """
        return self.session.query(Track).filter(
            Track.screen_name == name, Track.method == method).delete()

    @_commit
    def update_track(self, name: str, method: str, cur: int) -> None:
        """update 'track' with latest cursor

        :param name: twitter account screen name
        :param method: search function name
        :param cur: cursor number
        :return:
        """
        self.session.query(Track).filter(Track.screen_name == name,
                                         Track.method == method).delete()
        self.session.add(Track(name, method, cur))
