import datetime
import unittest

from app.models.dao import Dao
from app.models.tables import Tweeter, BaseTweeter, Track


class TestModel(unittest.TestCase):
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
        btw1 = BaseTweeter(id1)
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
        dao.bulk_save([btw1])
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
