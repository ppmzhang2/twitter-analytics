from config import Config
import twitter

__all__ = ['Tweet']


class SingletonMeta(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class Tweet(metaclass=SingletonMeta):
    __slots__ = ['api']

    def __init__(self):
        self.api = twitter.Api(consumer_key=Config.CONSUMER_KEY,
                               consumer_secret=Config.CONSUMER_SECRET,
                               access_token_key=Config.ACCESS_TOKEN,
                               access_token_secret=Config.ACCESS_TOKEN_SECRET)

    def get_followers(self,
                      user_id,
                      cursor=-1,
                      total_count=1000,
                      skip_status=True,
                      include_user_entities=False):
        self.api.GetFollowers(user_id=user_id,
                              cursor=cursor,
                              count=total_count,
                              total_count=total_count,
                              skip_status=skip_status,
                              include_user_entities=include_user_entities)

    def get_following(self,
                      user_id,
                      cursor=-1,
                      total_count=1000,
                      skip_status=True,
                      include_user_entities=False):
        self.api.GetFriends(user_id=user_id,
                            cursor=cursor,
                            count=total_count,
                            total_count=total_count,
                            skip_status=skip_status,
                            include_user_entities=include_user_entities)
