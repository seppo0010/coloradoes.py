import unittest

from .. import Coloradoes
from ..storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Coloradoes(Storage())
        self.values = []

    def test_sadd_smembers(self):
        self.values.append(self.database.command_sadd('key', '1', '2', '1'))
        self.values.append(self.database.command_smembers('key'))
        self.assertEqual(self.values, [2, ['1', '2']])

    def test_scard(self):
        self.values.append(self.database.command_sadd('key', '1', '2', '1'))
        self.values.append(self.database.command_sadd('key', '5', '3', '4'))
        self.values.append(self.database.command_scard('key'))
        self.assertEqual(self.values, [2, 3, 5])

    def test_sismember(self):
        self.values.append(self.database.command_sadd('key', '1', '2', '3'))
        self.values.append(self.database.command_sismember('key', '1'))
        self.values.append(self.database.command_sismember('key', '4'))
        self.assertEqual(self.values, [3, True, False])

    def test_srandmember(self):
        self.values.append(self.database.command_sadd('key', '1', '2', '3'))
        self.assertItemsEqual(self.database.command_srandmember('key', '3'),
                ('1', '2', '3'))
        for i in range(1, 100):
            self.assertIn(self.database.command_srandmember('key'), ('1',
                        '2', '3'))
        for i in range(1, 100):
            members = self.database.command_srandmember('key', '2')
            self.assertEquals(len(members), 2)
            self.assertLessEqual(set(members), set(('1', '2', '3')))

    def test_spop(self):
        self.database.command_sadd('key', '1', '2', '3')
        self.values.append(self.database.command_spop('key'))
        self.values.append(self.database.command_spop('key'))
        self.values.append(self.database.command_spop('key'))
        self.assertEquals(self.database.command_spop('key'), [])
        self.assertEquals(set(('1', '2', '3')), set(self.values))

    def test_srem(self):
        self.database.command_sadd('key', '1', '2', '3')
        self.values.append(self.database.command_srem('key', '3', '4'))
        self.values.append(self.database.command_smembers('key'))
        self.assertEqual(self.values, [1, ['1', '2']])

    def test_smove(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.values.append(self.database.command_smove('key1', 'key2', '3'))
        self.values.append(self.database.command_smove('key1', 'key2', '4'))
        self.values.append(self.database.command_smembers('key1'))
        self.values.append(self.database.command_smembers('key2'))
        self.assertEqual(self.values, [1, 0, ['1', '2'], ['3']])

    def test_sunion(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.database.command_sadd('key2', '3', '4', '5')
        union = self.database.command_sunion('key1', 'key2')
        self.assertEqual(len(union), 5)
        self.assertEqual(set(union), set(('1', '2', '3', '4', '5')))

    def test_sunionstore(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.database.command_sadd('key2', '3', '4', '5')
        self.database.command_sunionstore('key3', 'key1', 'key2')
        union = self.database.command_smembers('key3')
        self.assertEqual(len(union), 5)
        self.assertEqual(set(union), set(('1', '2', '3', '4', '5')))

    def test_sinter(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.database.command_sadd('key2', '3', '4', '5')
        inter = self.database.command_sinter('key1', 'key2')
        self.assertEqual(len(inter), 1)
        self.assertEqual(set(inter), set(('3')))
        self.database.command_sadd('key1', '4')
        inter = self.database.command_sinter('key1', 'key2')
        self.assertEqual(len(inter), 2)
        self.assertEqual(set(inter), set(('3', '4')))

    def test_sinterstore(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.database.command_sadd('key2', '3', '4', '5')
        inter = self.database.command_sinterstore('key3', 'key1', 'key2')
        self.assertEqual(inter, 1)
        self.assertEqual(self.database.command_smembers('key3'), ['3'])
        self.database.command_sadd('key1', '4')
        inter = self.database.command_sinterstore('key3', 'key1', 'key2')
        self.assertEqual(inter, 2)
        self.assertEqual(set(self.database.command_smembers('key3')),
                set(('3', '4')))

    def test_sdiff(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.database.command_sadd('key2', '3', '4', '5')
        diff = self.database.command_sdiff('key1', 'key2')
        self.assertEqual(len(diff), 2)
        self.assertEqual(set(diff), set(('1', '2')))

    def test_sdiffstore(self):
        self.database.command_sadd('key1', '1', '2', '3')
        self.database.command_sadd('key2', '3', '4', '5')
        diff = self.database.command_sdiffstore('key3', 'key1', 'key2')
        self.assertEqual(diff, 2)
        self.assertEqual(set(self.database.command_smembers('key3')),
                set(('1', '2')))
