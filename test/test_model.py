import unittest

from twitter.models import User

from app.models.dao import Dao
from app.models.tables import Track


class TestModel(unittest.TestCase):
    dao = None
    USER_IDS = (12345678901, 12345678902, 12345678903, 12345678904,
                12345678905)
    SCREEN_NAMES = ('user_1', 'user_2', 'user_3', 'user_4', 'user_5')
    NAMES = ('name 1', '五毛外宣部副部长\U0001f1e8\U0001f1f3', 'name 3', 'name 4',
             'name 5')
    DESCRIPTIONS = (
        '予人玫瑰 手有餘香', '北京摄影老顽童\U0001f1e8\U0001f1f3',
        'spread positive energy, let the world understand China! \n再艰难，爱不会离开！',
        '', '')
    CREATED_ATS = ('Tue Mar 29 08:11:25 +0000 2011',
                   'Tue Mar 29 08:11:25 +0000 2020',
                   'Tue Mar 29 08:11:25 +0000 2020',
                   'Tue Mar 29 08:11:25 +0000 2011',
                   'Tue Mar 29 08:11:25 +0000 2011')
    FOLLOWER_COUNTS = (1, 56, 878264, 223, 872)
    FRIEND_COUNTS = (3215, 0, 782, 3295, 3)
    METHODS = ('get_following_paged', 'get_followers_paged')
    CURSORS = (1656974570956055733, 1656809280611943888)
    USERS = [
        User(id=user_id,
             screen_name=screen_name,
             name=name,
             description=desc,
             created_at=dt,
             followers_count=follower_count,
             friends_count=friend_count)
        for user_id, screen_name, name, desc, dt, follower_count, friend_count
        in zip(USER_IDS, SCREEN_NAMES, NAMES, DESCRIPTIONS, CREATED_ATS,
               FOLLOWER_COUNTS, FRIEND_COUNTS)
    ]

    @classmethod
    def setUpClass(cls) -> None:
        print('setUpClass started')
        cls.dao = Dao()
        cls.dao.reset_db()
        print('setUpClass ended')

    @classmethod
    def tearDownClass(cls) -> None:
        print('tearDownClass started')
        cls.dao.reset_db()
        print('tearDownClass ended')

    def setUp(self):
        """save to DB, get variables of table instances:
        5 tweeters, 2 tracks, 3 wumaos
        instance variables:
          * tweeters
          * tracks
          * wumao_tweeter_ids
          * wumaos

        :return:
        """
        print('setUp started')

        tracks_ = [
            Track(user_id, method, cursor) for user_id, method, cursor in zip(
                self.USER_IDS, self.METHODS, self.CURSORS)
        ]
        self.dao.bulk_save_tweeter(self.USERS)
        self.dao.bulk_save(tracks_)
        tweeter_ids = self.dao.all_tweeter_id(self.USER_IDS)
        self.tweeters = [self.dao.lookup_tweeter(i) for i in tweeter_ids]
        self.tracks = [
            self.dao.lookup_track(track.user_id, track.method)
            for track in tracks_
        ]
        self.wumao_tweeter_ids = [u.id for u in self.tweeters][:3]
        self.dao.bulk_save_wumao(self.wumao_tweeter_ids)
        wumao_ids = self.dao.all_wumao_id()
        self.wumaos = [self.dao.lookup_wumao(i) for i in wumao_ids]
        print('setUp ended')

    def tearDown(self):
        print('tearDown started')
        self.dao.reset_db()
        print('tearDown ended')

    def test_bulk_save(self):
        """bulk save

        methods:
          * dao.bulk_save
          * dao.bulk_save_tweeter
        :return:
        """
        self.assertEqual(set(), self.dao.bulk_save_tweeter(self.USERS))
        self.assertEqual(set(),
                         self.dao.bulk_save_wumao(self.wumao_tweeter_ids))

    def test_tweeter(self):
        """checks DAO methods on table 'tweeter'

        methods:
          * dao.lookup_tweeter_id
          * dao.all_tweeter_user_id

        :return:
        """
        set_1 = set(
            self.dao.lookup_tweeter(tweeter.id).id
            for tweeter in self.tweeters)
        set_2 = self.dao.all_tweeter_id(TestModel.USER_IDS)
        self.assertEqual(set_1, set_2)

    def test_friendship(self):
        """checks many-to-many junction table 'friendship' and its 'on-delete'
        constrain

        friendship: 1 follows 2, 3, 4; 5 follows 1
        methods:
          * dao.follow
          * dao.bulk_follow
          * dao.bulk_attract
          * dao.friend_count
          * dao.follower_count
          * dao.delete_tweeter_id

        :return:
        """
        tweeter_id_1, tweeter_id_2, tweeter_id_3, tweeter_id_4, tweeter_id_5 = (
            u.id for u in self.tweeters)
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
        self.dao.delete_tweeter(tweeter_id_3)
        self.assertEqual([tweeter_id_2, tweeter_id_4],
                         self.dao.friends_id(tweeter_id_1))
        self.assertEqual([tweeter_id_5], self.dao.followers_id(tweeter_id_1))

    def test_wumao(self):
        """checks DAO methods of table 'wumao' and its 'on-delete' constrain

        methods
          * dao.all_wumao
          * dao.wumao_tweeter_ids
          * dao.delete_tweeter_id

        :return:
        """
        tweeter_id_1, tweeter_id_2, tweeter_id_3, tweeter_id_4, tweeter_id_5 = (
            u.id for u in self.tweeters)
        wumao_1, wumao_2, wumao_3 = self.wumaos
        self.assertEqual(
            set(self.wumao_tweeter_ids),
            {wumao_1.tweeter_id, wumao_2.tweeter_id, wumao_3.tweeter_id})
        # on-delete constrain
        self.dao.delete_tweeter(tweeter_id_3)
        self.assertEqual({wumao_1.id, wumao_2.id}, self.dao.all_wumao_id())

    def test_track(self):
        """checks DAO methods of table 'track'

        methods:
          * dao.lookup_track
          * dao.delete_track
          * dao.update_track

        :return:
        """
        # initialize & preparation
        track_1, track_2 = self.tracks
        # delete
        self.assertEqual(set(TestModel.CURSORS),
                         {track_1.cursor, track_2.cursor})
        self.dao.delete_track(track_1.user_id, track_1.method)
        self.assertEqual(
            None, self.dao.lookup_track(track_1.user_id, track_1.method))
        # multiple tracks update
        self.dao.update_track(track_1.user_id, track_1.method, track_1.cursor)
        self.dao.update_track(track_1.user_id, track_1.method, track_1.cursor)
        self.dao.update_track(track_2.user_id, track_2.method, track_2.cursor)
        self.dao.update_track(track_2.user_id, track_2.method, track_2.cursor)
        self.assertEqual(
            track_1.cursor,
            self.dao.lookup_track(track_1.user_id, track_1.method).cursor)
        self.assertEqual(
            track_2.cursor,
            self.dao.lookup_track(track_2.user_id, track_2.method).cursor)


if __name__ == '__main__':
    unittest.main(verbosity=2)
