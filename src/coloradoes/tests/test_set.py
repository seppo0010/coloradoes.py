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
