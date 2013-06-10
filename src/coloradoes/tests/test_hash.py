import time
import unittest

from .. import Coloradoes
from ..storage.memory import Storage


class TestHash(unittest.TestCase):
    def setUp(self):
        super(TestHash, self).setUp()
        self.database = Coloradoes(Storage())
        self.values = []

    def test_hset_hget(self):
        self.values.append(self.database.command_hget('key', 'field'))
        self.values.append(self.database.command_hset('key', 'field', 'value'))
        self.values.append(self.database.command_hget('key', 'field'))
        self.assertEqual(self.values, [None, True, 'value'])

    def test_hset_hdel(self):
        self.values.append(self.database.command_hset('key', 'field1',
                    'value1'))
        self.values.append(self.database.command_hset('key', 'field2',
                    'value2'))
        self.values.append(self.database.command_hdel('key', 'field0',
                    'field1', 'field3'))
        self.values.append(self.database.command_hget('key', 'field2'))
        self.assertEqual(self.values, [True, True, 1, 'value2'])

    def test_hset_hexists(self):
        self.values.append(self.database.command_hset('key', 'field1',
                    'value1'))
        self.values.append(self.database.command_hexists('key', 'field1'))
        self.values.append(self.database.command_hexists('key', 'field2'))
        self.assertEqual(self.values, [True, True, False])

    def test_hset_hgetall(self):
        self.values.append(self.database.command_hset('key', 'field1',
                    'value1'))
        self.values.append(self.database.command_hset('key', 'field2',
                    'value2'))
        self.values.append(self.database.command_hgetall('key'))
        self.assertEqual(self.values, [True, True, ['field1', 'value1',
                'field2', 'value2']])

    def test_hincrby(self):
        self.values.append(self.database.command_hincrby('key', 'field', '1'))
        self.values.append(self.database.command_hincrby('key', 'field', '1'))
        self.values.append(self.database.command_hget('key', 'field'))
        self.assertEqual(self.values, [1, 2, '2'])

    def test_hincrbyfloat(self):
        self.values.append(self.database.command_hincrbyfloat('key', 'field',
                    '1.5'))
        self.values.append(self.database.command_hincrbyfloat('key', 'field',
                    '1.2'))
        self.values.append(self.database.command_hget('key', 'field'))
        self.assertEqual(self.values, [1.5, 2.7, '2.7'])

    def test_hset_hkeys(self):
        self.values.append(self.database.command_hset('key', 'field1',
                    'value1'))
        self.values.append(self.database.command_hset('key', 'field2',
                    'value2'))
        self.values.append(self.database.command_hkeys('key'))
        self.assertEqual(self.values, [True, True, ['field1', 'field2']])

    def test_hset_hlen(self):
        self.values.append(self.database.command_hset('key', 'field1',
                    'value1'))
        self.values.append(self.database.command_hset('key', 'field2',
                    'value2'))
        self.values.append(self.database.command_hlen('key'))
        self.assertEqual(self.values, [True, True, 2])

    def test_hset_hmget(self):
        self.values.append(self.database.command_hset('key', 'field1',
                    'value1'))
        self.values.append(self.database.command_hset('key', 'field2',
                    'value2'))
        self.values.append(self.database.command_hmget('key', 'field1',
                    'field2', 'field3'))
        self.assertEqual(self.values, [True, True, ['value1', 'value2', None]])

    def test_hmset_hmget(self):
        self.values.append(self.database.command_hmset('key', 'field1',
                    'value1', 'field2', 'value2'))
        self.values.append(self.database.command_hmget('key', 'field1',
                    'field2'))
        self.assertEqual(self.values, [True, ['value1', 'value2']])
