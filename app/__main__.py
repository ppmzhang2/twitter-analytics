from app.models.dao import Dao
from app.tweet import Tweet
from app.models.tables import Tweeter, BaseTweeter
from config import Config
import twitter


def user_to_tweeter(user: twitter.models.User):
    """convert a twitter.User instance to a Tweeter ORM object

    :param user: a twitter.User instance
    :return: a Tweeter object
    """
    return Tweeter(user.id, user.screen_name, user.name,
                   Tweet.parse_date(user.created_at), user.followers_count,
                   user.friends_count)


def user_to_base_tweeter(user: twitter.User):
    """convert a twitter.User instance to a BaseTweeter ORM object

    :param user: a twitter.User instance
    :return: a Tweeter object
    """
    return BaseTweeter(user.id)


def save_init_wumao():
    dao = Dao(new=False)
    tweet = Tweet()
    wumaos = [
        u for u in tweet.get_followers(user_id=Config.TWEET_ENTRY_USER_ID)
        if Tweet.is_junior_wumao(u)
    ]
    tweeter_wumao = [user_to_tweeter(u) for u in wumaos]
    base_tweeter_wumao = [user_to_base_tweeter(u) for u in wumaos]

    dao.bulk_save(tweeter_wumao)
    dao.bulk_save(base_tweeter_wumao)


if __name__ == '__main__':
    save_init_wumao()
