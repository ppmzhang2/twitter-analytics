from functools import wraps

from app.models.base import session_factory
from app.models.tables import Tweeter

__all__ = ['Dao']


def _commit(fn):
    @wraps(fn)
    def helper(*args, **kwargs):
        fn(*args, **kwargs)
        args[0].session.commit()

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
    def bulk_save(self, objects):
        """Perform a bulk save of the given sequence of objects

        :param objects: a sequence of mapped object instances
        :return:
        """
        self.session.bulk_save_objects(objects)

    def lookup_tweeter_user_id(self, user_id):
        return self.session.query(Tweeter).filter(
            Tweeter.user_id == user_id).first()

    @_commit
    def delete_tweeter_user_id(self, user_id):
        return self.session.query(Tweeter).filter(
            Tweeter.user_id == user_id).delete()
