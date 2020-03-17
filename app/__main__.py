from time import sleep

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


def wumao_from_base_top(dao: Dao, tweet: Tweet):
    user_id = dao.first_base_tweeter().user_id
    print("potential wumao:", user_id)
    tweeters = [u for u in tweet.get_following(user_id=user_id)
                ] + [u for u in tweet.get_followers(user_id=user_id)]
    wumaos = [u for u in tweeters if tweet.is_junior_wumao(u)]
    unique_wumaos = [
        u for u in wumaos if dao.lookup_tweeter_user_id(u.id) is None
    ]
    if not wumaos:
        print("NOT wumao")
        dao.delete_tweeter_user_id(user_id)
        dao.delete_base_tweeter_user_id(user_id)
    elif not unique_wumaos:
        print("no additional wumao")
        dao.delete_base_tweeter_user_id(user_id)
    else:
        print("inserting new wumaos:", unique_wumaos)
        tweeter_wumao = [user_to_tweeter(u) for u in unique_wumaos]
        base_tweeter_wumao = [user_to_base_tweeter(u) for u in unique_wumaos]
        dao.bulk_save(tweeter_wumao)
        dao.bulk_save(base_tweeter_wumao)
        dao.delete_base_tweeter_user_id(user_id)


def wumao_loop():
    d = Dao(new=False)
    t = Tweet()
    while True:
        try:
            wumao_from_base_top(d, t)
        except twitter.error.TwitterError as e:
            if e.message[0]['code'] == 88:
                # sleep 6 min if exceeds limit
                sleep(360)
            else:
                # else raise error
                raise e


if __name__ == '__main__':
    # save_init_wumao()
    wumao_loop()
