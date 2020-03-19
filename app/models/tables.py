from datetime import date

import sqlalchemy as sa

from app.models.base import Base


class Tweeter(Base):
    """table to store Wumao users
    """
    __tablename__ = 'tweeter'

    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.BigInteger)
    screen_name = sa.Column(sa.String)
    name = sa.Column(sa.String)
    created_at = sa.Column(sa.Date)
    followers = sa.Column(sa.Integer)
    following = sa.Column(sa.Integer)

    def __init__(self, user_id: int, screen_name: str, name: str,
                 created_at: date, followers: int, following: int):
        self.user_id = user_id
        self.screen_name = screen_name
        self.name = name
        self.created_at = created_at
        self.followers = followers
        self.following = following


class BaseTweeter(Base):
    """Tweeter ID list from which to search
    """
    __tablename__ = 'base_tweeter'

    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.BigInteger)

    def __init__(self, user_id: int):
        self.user_id = user_id


class Track(Base):
    """tracks from which to restore twitter paged search
    """
    __tablename__ = 'cursor'

    id = sa.Column(sa.Integer, primary_key=True)
    cursor = sa.Column(sa.BigInteger)

    def __init__(self, cursor: int):
        self.cursor = cursor
