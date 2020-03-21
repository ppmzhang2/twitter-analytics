import datetime
import unittest

from app.models.dao import Dao
from app.models.tables import Tweeter, Friendship, Track


class TestModel(unittest.TestCase):
    def test_friendship(self):
        user_id_1 = 12345678901
        user_id_2 = 12345678902
        user_id_3 = 12345678903
        user_id_4 = 12345678904
        user_id_5 = 12345678905
        tw1 = Tweeter(user_id_1, 'usr1', 'name1', datetime.date(2020, 10, 3),
                      2, 39)
        tw2 = Tweeter(user_id_2, 'usr2', 'name2', datetime.date(2019, 1, 23),
                      21, 9)
        tw3 = Tweeter(user_id_3, 'usr3', 'name3', datetime.date(2019, 2, 23),
                      22, 8)
        tw4 = Tweeter(user_id_4, 'usr4', 'name4', datetime.date(2019, 3, 23),
                      23, 7)
        tw5 = Tweeter(user_id_5, 'usr5', 'name5', datetime.date(2019, 4, 23),
                      24, 6)
        dao = Dao(new=False)
        # bulk save
        dao.bulk_save([tw1, tw2, tw3, tw4, tw5])
        # find PK_ID
        tweeter_1 = dao.lookup_tweeter_user_id(user_id_1)
        tweeter_2 = dao.lookup_tweeter_user_id(user_id_2)
        tweeter_3 = dao.lookup_tweeter_user_id(user_id_3)
        tweeter_4 = dao.lookup_tweeter_user_id(user_id_4)
        tweeter_5 = dao.lookup_tweeter_user_id(user_id_5)
        # follow
        dao.follow(tweeter_1.id, tweeter_2.id)
        dao.follow(tweeter_1.id, tweeter_3.id)
        dao.follow(tweeter_1.id, tweeter_4.id)
        dao.follow(tweeter_5.id, tweeter_1.id)
        # check friendship
        self.assertEqual(True, dao.is_following(tweeter_1.id, tweeter_2.id))
        self.assertEqual(True, dao.is_following(tweeter_1.id, tweeter_3.id))
        self.assertEqual(True, dao.is_following(tweeter_1.id, tweeter_4.id))
        self.assertEqual(False, dao.is_following(tweeter_1.id, tweeter_5.id))
        self.assertEqual(True, dao.is_following(tweeter_5.id, tweeter_1.id))
        # delete 3rd
        dao.delete_tweeter_id(tweeter_3.id)
        self.assertEqual([tweeter_2.id, tweeter_4.id],
                         dao.friends_id(tweeter_1.id))
        self.assertEqual([tweeter_5.id], dao.followers_id(tweeter_1.id))
        # delete
        dao.delete_tweeter_id(tweeter_1.id)
        dao.delete_tweeter_id(tweeter_2.id)
        dao.delete_tweeter_id(tweeter_3.id)
        dao.delete_tweeter_id(tweeter_4.id)
        dao.delete_tweeter_id(tweeter_5.id)

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
