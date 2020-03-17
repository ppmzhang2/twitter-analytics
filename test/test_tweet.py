import datetime
import unittest

from twitter.models import User

from app.tweet import Tweet


class TestTweet(unittest.TestCase):
    def test_parse_date(self):
        ts_str = 'Tue Mar 29 08:11:25 +0000 2011'
        dt = datetime.date(2011, 3, 29)
        # check existence
        self.assertEqual(Tweet.parse_date(ts_str), dt)

    def test_is_junior_wumao(self):
        wumao1 = User(id=251,
                      screen_name='wumao1',
                      created_at='Tue Mar 29 08:11:25 +0000 2011',
                      followers_count=4)
        wumao2 = User(id=252,
                      screen_name='wumao2',
                      created_at='Tue Mar 29 08:11:25 +0000 2020',
                      followers_count=8)
        human1 = User(id=101,
                      screen_name='human1',
                      created_at='Tue Mar 29 08:11:25 +0000 2020',
                      followers_count=30)
        human2 = User(id=102,
                      screen_name='human2',
                      created_at='Tue Mar 29 08:11:25 +0000 2011',
                      followers_count=8)
        self.assertEqual(Tweet.is_junior_wumao(wumao1), True)
        self.assertEqual(Tweet.is_junior_wumao(wumao2), True)
        self.assertEqual(Tweet.is_junior_wumao(human1), False)
        self.assertEqual(Tweet.is_junior_wumao(human2), False)


if __name__ == '__main__':
    unittest.main(verbosity=2)
