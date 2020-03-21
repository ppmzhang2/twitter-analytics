import datetime
import unittest
from datetime import date

from app.models.dao import Dao
from app.models.tables import Tweeter, Track


class TestModel(unittest.TestCase):
    USER_IDS = (12345678901, 12345678902, 12345678903, 12345678904,
                12345678905)
    SCREEN_NAMES = ('user_1', 'user_2', 'user_3', 'user_4', 'user_5')
    NAMES = ('name 1', 'name 2', 'name 3', 'name 4', 'name 5')
    DATES = (date(2011, 12, 16), date(2020, 2, 29), date(2018, 1, 2),
             date(2017, 6, 15), date(1991, 6, 25))
    FOLLOWER_COUNTS = (1, 56, 878264, 223, 872)
    FRIEND_COUNTS = (3215, 0, 782, 3295, 3)
    TWEETERS = (Tweeter(user_id, screen_name, name, dt, follower_count,
                        friend_count)
                for user_id, screen_name, name, dt, follower_count,
                friend_count in zip(USER_IDS, SCREEN_NAMES, NAMES, DATES,
                                    FOLLOWER_COUNTS, FRIEND_COUNTS))

    def test_friendship(self):
        """checks DAO methods on many-to-many junction 'friendship' and its
        'on-delete' constrain

        :return:
        """
        dao = Dao(new=False)
        # bulk save
        dao.bulk_save(TestModel.TWEETERS)
        # find record IDs from table 'tweeter'
        tweeter_id_1, tweeter_id_2, tweeter_id_3, tweeter_id_4, tweeter_id_5 = (
            dao.lookup_tweeter_user_id(user_id).id
            for user_id in TestModel.USER_IDS)
        # follow: 1 follows 2, 3, 4; 5 follows 1
        dao.follow(tweeter_id_1, tweeter_id_2)
        dao.follow(tweeter_id_1, tweeter_id_3)
        dao.follow(tweeter_id_1, tweeter_id_4)
        dao.follow(tweeter_id_5, tweeter_id_1)
        # check friendship
        self.assertEqual(True, dao.is_following(tweeter_id_1, tweeter_id_2))
        self.assertEqual(True, dao.is_following(tweeter_id_1, tweeter_id_3))
        self.assertEqual(True, dao.is_following(tweeter_id_1, tweeter_id_4))
        self.assertEqual(False, dao.is_following(tweeter_id_1, tweeter_id_5))
        self.assertEqual(True, dao.is_following(tweeter_id_5, tweeter_id_1))
        # delete 3
        dao.delete_tweeter_id(tweeter_id_3)
        self.assertEqual([tweeter_id_2, tweeter_id_4],
                         dao.friends_id(tweeter_id_1))
        self.assertEqual([tweeter_id_5], dao.followers_id(tweeter_id_1))
        # delete
        dao.delete_tweeter_id(tweeter_id_1)
        dao.delete_tweeter_id(tweeter_id_2)
        dao.delete_tweeter_id(tweeter_id_3)
        dao.delete_tweeter_id(tweeter_id_4)
        dao.delete_tweeter_id(tweeter_id_5)

    def test_dao(self):
        id1 = 12345
        id2 = 34567
        user_id_1 = 9876543210
        user_id_2 = 123456789
        method_1 = 'get_following_paged'
        method_2 = 'get_followers_paged'
        cursor_1 = 1656974570956055733
        cursor_2 = 1656809280611943888
        tw1 = Tweeter(id1, 'usr1', 'name1', datetime.date(2020, 10, 3), 2, 39)
        tw2 = Tweeter(id2, 'usr2', 'name2', datetime.date(2019, 1, 23), 20, 9)
        track1 = Track(user_id_1, method_1, cursor_1)
        dao = Dao(new=False)
        # first delete with no data in DB
        dao.delete_tweeter_user_id(id1)
        dao.delete_tweeter_user_id(id2)
        dao.delete_base_tweeter_user_id(id1)
        dao.delete_track(user_id_1, method_1)
        dao.delete_track(user_id_2, method_2)
        # bulk save
        dao.bulk_save([tw1, tw2])
        dao.bulk_save([track1])
        # check existence
        self.assertEqual(dao.lookup_tweeter_user_id(id1).user_id, id1)
        self.assertEqual(dao.lookup_tweeter_user_id(id2).user_id, id2)
        self.assertEqual(dao.first_base_tweeter().user_id, id1)
        self.assertEqual(
            dao.lookup_track(user_id_1, method_1).cursor, cursor_1)
        self.assertEqual(dao.lookup_track(user_id_2, method_2), None)
        # multiple tracks update
        dao.update_track(user_id_1, method_1, cursor_1)
        dao.update_track(user_id_1, method_1, cursor_1)
        dao.update_track(user_id_2, method_2, cursor_2)
        dao.update_track(user_id_2, method_2, cursor_2)
        # check existence
        self.assertEqual(
            dao.lookup_track(user_id_1, method_1).cursor, cursor_1)
        self.assertEqual(
            dao.lookup_track(user_id_2, method_2).cursor, cursor_2)
        # delete again
        dao.delete_tweeter_user_id(id1)
        dao.delete_tweeter_user_id(id2)
        dao.delete_base_tweeter_user_id(id1)
        dao.delete_track(user_id_1, method_1)
        dao.delete_track(user_id_2, method_2)
        # check existence after deletion
        self.assertEqual(dao.lookup_tweeter_user_id(id1), None)
        self.assertEqual(dao.lookup_tweeter_user_id(id2), None)
        self.assertEqual(dao.first_base_tweeter(), None)
        self.assertEqual(dao.lookup_track(user_id_1, method_1), None)
        self.assertEqual(dao.lookup_track(user_id_2, method_2), None)


if __name__ == '__main__':
    unittest.main(verbosity=2)
