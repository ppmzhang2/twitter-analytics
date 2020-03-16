import datetime
import unittest

from app.models.dao import Dao
from app.models.tables import Tweeter


class TestModel(unittest.TestCase):
    def test_dao(self):
        id1 = 12345
        id2 = 34567
        tw1 = Tweeter(id1, 'usr1', 'name1', datetime.date(2020, 10, 3), 2, 39)
        tw2 = Tweeter(id2, 'usr2', 'name2', datetime.date(2019, 1, 23), 20, 9)
        dao = Dao(new=False)
        # bulk save
        dao.bulk_save([tw1, tw2])
        # check existence
        self.assertEqual(dao.lookup_tweeter_user_id(id1).user_id, id1)
        self.assertEqual(dao.lookup_tweeter_user_id(id2).user_id, id2)
        # delete
        dao.delete_tweeter_user_id(id1)
        dao.delete_tweeter_user_id(id2)
        # check existence after deletion
        self.assertEqual(dao.lookup_tweeter_user_id(id1), None)
        self.assertEqual(dao.lookup_tweeter_user_id(id2), None)


if __name__ == '__main__':
    unittest.main(verbosity=2)
