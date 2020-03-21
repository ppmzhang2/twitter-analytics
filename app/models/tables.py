from datetime import date

import sqlalchemy as sa
from sqlalchemy import ForeignKey

from app.models.base import Base


class Friendship(Base):
    __tablename__ = 'friendship'

    author_id = sa.Column(sa.Integer,
                          ForeignKey('tweeter.id',
                                     onupdate='CASCADE',
                                     ondelete='CASCADE'),
                          index=True,
                          primary_key=True)
    follower_id = sa.Column(sa.Integer,
                            ForeignKey('tweeter.id',
                                       onupdate='CASCADE',
                                       ondelete='CASCADE'),
                            index=True,
                            primary_key=True)

    def __init__(self, author_id: int, follower_id: int):
        self.author_id = author_id
        self.follower_id = follower_id


class Tweeter(Base):
    """table to store Wumao users
    """
    __tablename__ = 'tweeter'

    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.BigInteger, unique=True, index=True)
    screen_name = sa.Column(sa.String)
    name = sa.Column(sa.String)
    created_at = sa.Column(sa.Date)
    follower_count = sa.Column(sa.Integer)
    friend_count = sa.Column(sa.Integer)

    def __init__(self, user_id: int, screen_name: str, name: str,
                 created_at: date, followers: int, following: int):
        self.user_id = user_id
        self.screen_name = screen_name
        self.name = name
        self.created_at = created_at
        self.follower_count = followers
        self.friend_count = following


class Track(Base):
    """tracks from which to restore twitter paged search
    """
    __tablename__ = 'track'

    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.BigInteger)
    method = sa.Column(sa.String)
    cursor = sa.Column(sa.BigInteger)

    def __init__(self, user_id: int, method: str, cursor: int):
        self.user_id = user_id
        self.method = method
        self.cursor = cursor
