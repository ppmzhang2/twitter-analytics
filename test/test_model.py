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
    METHODS = ('get_following_paged', 'get_followers_paged')
    CURSORS = (1656974570956055733, 1656809280611943888)

    @classmethod
    def setUpClass(cls) -> None:
        print('setUpClass started')
        cls.dao = Dao()
        cls.dao.reset_db()
        tweeters_ = [
            Tweeter(user_id, screen_name, name, dt, follower_count,
                    friend_count)
            for user_id, screen_name, name, dt, follower_count, friend_count in
            zip(cls.USER_IDS, cls.SCREEN_NAMES, cls.NAMES, cls.DATES,
                cls.FOLLOWER_COUNTS, cls.FRIEND_COUNTS)
        ]
        tracks_ = [
            Track(user_id, method, cursor) for user_id, method, cursor in zip(
                cls.USER_IDS, cls.METHODS, cls.CURSORS)
        ]
        # get class variables of table instances
        # bulk save: 5 tweeters, 2 tracks
        # methods
        #   1. dao.bulk_save
        #   2. dao.bulk_save_tweeter
        cls.dao.bulk_save_tweeter(tweeters_)
        cls.dao.bulk_save_tweeter(tweeters_)
        cls.dao.bulk_save(tracks_)
        cls.tweeters = cls.dao.all_tweeter_user_id(cls.USER_IDS)
        cls.tracks = [
            cls.dao.lookup_track(track.user_id, track.method)
            for track in tracks_
        ]
        cls.wumao_tweeter_ids = [u.id for u in cls.tweeters][:3]
        cls.dao.bulk_save_wumao(cls.wumao_tweeter_ids)
        cls.wumaos = cls.dao.all_wumao()
        print('setUpClass ended')

    @classmethod
    def tearDownClass(cls) -> None:
        print('tearDownClass started')
        cls.dao.reset_db()
        print('tearDownClass ended')

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_tweeter(self):
        """checks DAO methods on table 'tweeter', its many-to-many junction
        table 'friendship' and its 'on-delete' constrain

        :return:
        """
        # initialize & preparation
        tweeter_id_1, tweeter_id_2, tweeter_id_3, tweeter_id_4, tweeter_id_5 = (
            u.id for u in self.tweeters)
        # find all tweeter
        # methods:
        #   1. dao.lookup_tweeter_user_id
        #   2. dao.all_tweeter_user_id
        set_1 = set(
            self.dao.lookup_tweeter_user_id(user_id)
            for user_id in TestModel.USER_IDS)
        set_2 = set(self.dao.all_tweeter_user_id(TestModel.USER_IDS))
        self.assertEqual(set_1, set_2)
        # follow: 1 follows 2, 3, 4; 5 follows 1
        # methods:
        #   1. dao.follow
        #   2. dao.bulk_follow
        #   3. dao.bulk_attract
        #   4. dao.friend_count
        #   5. dao.follower_count
        self.dao.follow(tweeter_id_5, tweeter_id_1)
        self.dao.bulk_follow(tweeter_id_1, [tweeter_id_2, tweeter_id_3])
        self.dao.bulk_attract(tweeter_id_4, [tweeter_id_1])
        self.assertEqual(True, self.dao.is_following(tweeter_id_1,
                                                     tweeter_id_2))
        self.assertEqual(True, self.dao.is_following(tweeter_id_1,
                                                     tweeter_id_3))
        self.assertEqual(True, self.dao.is_following(tweeter_id_1,
                                                     tweeter_id_4))
        self.assertEqual(False,
                         self.dao.is_following(tweeter_id_1, tweeter_id_5))
        self.assertEqual(True, self.dao.is_following(tweeter_id_5,
                                                     tweeter_id_1))
        self.assertEqual(3, self.dao.friend_count(tweeter_id_1))
        self.assertEqual(
            2, self.dao.friend_count(tweeter_id_1,
                                     [tweeter_id_2, tweeter_id_4]))
        self.assertEqual(1, self.dao.follower_count(tweeter_id_1))
        self.assertEqual(0,
                         self.dao.follower_count(tweeter_id_1, [tweeter_id_1]))
        self.assertEqual(0, self.dao.follower_count(999))
        # on-delete constrain
        # methods:
        #   1. dao.delete_tweeter_id
        #   2. dao.all_wumao
        wumao_1, wumao_2, wumao_3 = self.wumaos
        self.assertEqual(
            set(self.wumao_tweeter_ids),
            {wumao_1.tweeter_id, wumao_2.tweeter_id, wumao_3.tweeter_id})
        self.dao.delete_tweeter_id(tweeter_id_3)
        self.assertEqual([tweeter_id_2, tweeter_id_4],
                         self.dao.friends_id(tweeter_id_1))
        self.assertEqual([tweeter_id_5], self.dao.followers_id(tweeter_id_1))
        self.assertEqual({wumao_1, wumao_2}, set(self.dao.all_wumao()))

    def test_track(self):
        # initialize & preparation
        track_1, track_2 = self.tracks
        # delete
        # methods:
        #   1. dao.lookup_track
        #   2. dao.delete_track
        self.assertEqual(set(TestModel.CURSORS),
                         {track_1.cursor, track_2.cursor})
        self.dao.delete_track(track_1.user_id, track_1.method)
        self.assertEqual(
            None, self.dao.lookup_track(track_1.user_id, track_1.method))
        # multiple tracks update
        # methods:
        #   1. dao.update_track
        self.dao.update_track(track_1.user_id, track_1.method, track_1.cursor)
        self.dao.update_track(track_1.user_id, track_1.method, track_1.cursor)
        self.dao.update_track(track_2.user_id, track_2.method, track_2.cursor)
        self.dao.update_track(track_2.user_id, track_2.method, track_2.cursor)
        # check existence
        self.assertEqual(
            track_1.cursor,
            self.dao.lookup_track(track_1.user_id, track_1.method).cursor)
        self.assertEqual(
            track_2.cursor,
            self.dao.lookup_track(track_2.user_id, track_2.method).cursor)


if __name__ == '__main__':
    unittest.main(verbosity=2)
