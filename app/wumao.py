from config import Config as cfg
import twitter


class WumaoApi(object):
    pass


if __name__ == '__main__':
    api = twitter.Api(consumer_key=cfg.CONSUMER_KEY,
                      consumer_secret=cfg.CONSUMER_SECRET,
                      access_token_key=cfg.ACCESS_TOKEN,
                      access_token_secret=cfg.ACCESS_TOKEN_SECRET)

    res = api.GetFollowers(user_id=141627220,
                           cursor=-1,
                           count=200,
                           total_count=400,
                           skip_status=True,
                           include_user_entities=False)

    res2 = api.GetFollowers(user_id=141627220,
                            cursor=0,
                            count=200,
                            total_count=400,
                            skip_status=True,
                            include_user_entities=False)
