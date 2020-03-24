import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    # credentials
    CONSUMER_KEY = ''
    CONSUMER_SECRET = ''
    ACCESS_TOKEN = ''
    ACCESS_TOKEN_SECRET = ''
    # path
    DB_DIR = ''.join([basedir, '/db'])
    APP_DB = ''.join([DB_DIR, '/app.db'])
    BAK_DB = ''.join([DB_DIR, '/app.db.bak'])
    WUMAO_CSV = ''.join([basedir, '/wumao.csv'])
