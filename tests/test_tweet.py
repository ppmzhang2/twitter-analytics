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


if __name__ == '__main__':
    unittest.main(verbosity=2)
