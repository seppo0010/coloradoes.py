import time
import unittest

from redislit3 import Redislit3
from redislit3.storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Redislit3(Storage())
        self.values = []

    def test_set_get(self):
        self.values.append(self.database.command_get('key'))
        self.values.append(self.database.command_set('key', 'value'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [None, True, 'value'])

    def test_setnx(self):
        self.values.append(self.database.command_set('key', '0'))
        self.values.append(self.database.command_setnx('key', 'value'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [True, False, '0'])

    def test_set_del_get(self):
        self.values.append(self.database.command_set('key', 'value'))
        self.values.append(self.database.command_del('key'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [True, 1, None])

    def test_del_multiple_keys(self):
        self.values.append(self.database.command_set('key1', 'value'))
        self.values.append(self.database.command_set('key2', 'value'))
        self.values.append(self.database.command_del('key1', 'key2',
                    'key3'))
        self.assertEqual(self.values, [True, True, 2])

    def test_getrange(self):
        self.values.append(self.database.command_set('key', 'value'))
        self.values.append(self.database.command_getrange('key'))
        self.values.append(self.database.command_getrange('key', 0))
        self.values.append(self.database.command_getrange('key', 1))
        self.values.append(self.database.command_getrange('key', 0, 0))
        self.values.append(self.database.command_getrange('key', 1, 0))
        self.values.append(self.database.command_getrange('key', 0, -1))
        self.values.append(self.database.command_getrange('key', 0, -2))
        self.assertEqual(self.values, [True, 'value', 'value', 'alue', 'v', '',
                'value', 'valu'])

    def test_getset(self):
        self.values.append(self.database.command_set('key', 'value'))
        self.values.append(self.database.command_getset('key', 'value2'))
        self.values.append(self.database.command_getset('key', 'value3'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [True, 'value', 'value2', 'value3'])

    def test_increment(self):
        self.values.append(self.database.command_incr('key'))
        self.values.append(self.database.command_incr('key'))
        self.assertEqual(self.values, ['1', '2'])

    def test_decrement(self):
        self.values.append(self.database.command_decr('key'))
        self.values.append(self.database.command_decr('key'))
        self.assertEqual(self.values, ['-1', '-2'])

    def test_incrby(self):
        self.values.append(self.database.command_incrby('key', '5'))
        self.values.append(self.database.command_incrby('key', '10'))
        self.assertEqual(self.values, ['5', '15'])

    def test_decrement(self):
        self.values.append(self.database.command_decrby('key', '5'))
        self.values.append(self.database.command_decrby('key', '10'))
        self.assertEqual(self.values, ['-5', '-15'])

    def test_mset_invalid(self):
        with self.assertRaises(ValueError):
            self.database.command_mset('key', 'value', 'key2')

    def test_mset(self):
        self.values.append(self.database.command_set('key', '0'))
        self.values.append(self.database.command_mset('key', 'value',
                    'key2', 'value2'))
        self.values.append(self.database.command_get('key'))
        self.values.append(self.database.command_get('key2'))
        self.assertEqual(self.values, [True, 2, 'value', 'value2'])

    def test_msetnx(self):
        self.values.append(self.database.command_set('key', '0'))
        self.values.append(self.database.command_msetnx('key', 'value',
                    'key2', 'value2'))
        self.values.append(self.database.command_get('key'))
        self.values.append(self.database.command_get('key2'))
        self.assertEqual(self.values, [True, 1, '0', 'value2'])

    def test_mget(self):
        self.values.append(self.database.command_set('key0', '0'))
        self.values.append(self.database.command_set('key1', '1'))
        self.values.append(self.database.command_set('key2', '2'))
        self.values.append(self.database.command_set('key3', '3'))
        self.values.append(self.database.command_mget('key0', 'key1', 'key3',
                    'key2'))
        self.assertEqual(self.values, [True, True, True, True,
                ['0', '1', '3', '2']])
