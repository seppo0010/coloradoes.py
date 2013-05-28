import unittest

from .. import Coloradoes
from ..storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Coloradoes(Storage())
        self.values = []

    def test_lpush(self):
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.values.append(self.database.command_lpush('key', 'value1'))
        self.values.append(self.database.command_lpush('key', 'value2'))
        self.values.append(self.database.command_lpush('key', 'value3'))
        self.values.append(self.database.command_lpush('key', 'value4'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.assertEqual(self.values, [[], None, None, None, None,
                ['value4', 'value3', 'value2', 'value1']])

    def test_lrange(self):
        self.values.append(self.database.command_lpush('key', 'value1'))
        self.values.append(self.database.command_lpush('key', 'value2'))
        self.values.append(self.database.command_lpush('key', 'value3'))
        self.values.append(self.database.command_lpush('key', 'value4'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.values.append(self.database.command_lrange('key', 0, -2))
        self.values.append(self.database.command_lrange('key', 0, -3))
        self.values.append(self.database.command_lrange('key', 0, -4))
        self.values.append(self.database.command_lrange('key', 0, -5))
        self.values.append(self.database.command_lrange('key', -1, -1))
        self.values.append(self.database.command_lrange('key', -2, -1))
        self.values.append(self.database.command_lrange('key', 1, -1))
        self.values.append(self.database.command_lrange('key', 2, -1))
        self.assertEqual(self.values, [None, None, None, None,
                ['value4', 'value3', 'value2', 'value1'],
                ['value4', 'value3', 'value2'],
                ['value4', 'value3'],
                ['value4'],
                [],
                ['value1'],
                ['value2', 'value1'],
                ['value3', 'value2', 'value1'],
                ['value2', 'value1'],
                ])

    def test_rpush(self):
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.values.append(self.database.command_rpush('key', 'value1'))
        self.values.append(self.database.command_rpush('key', 'value2'))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_rpush('key', 'value4'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.assertEqual(self.values, [[], None, None, None, None,
                ['value1', 'value2', 'value3', 'value4']])

    def test_lpush_rpush(self):
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_lpush('key', 'value2'))
        self.values.append(self.database.command_rpush('key', 'value4'))
        self.values.append(self.database.command_lpush('key', 'value1'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.assertEqual(self.values, [[], None, None, None, None,
                ['value1', 'value2', 'value3', 'value4']])
