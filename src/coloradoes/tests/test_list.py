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

    def test_lindex(self):
        self.values.append(self.database.command_rpush('key', 'value1'))
        self.values.append(self.database.command_rpush('key', 'value2'))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_rpush('key', 'value4'))
        self.values.append(self.database.command_lindex('key', 0))
        self.values.append(self.database.command_lindex('key', 3))
        self.values.append(self.database.command_lindex('key', -1))
        self.values.append(self.database.command_lindex('key', -5))
        self.assertEqual(self.values, [None, None, None, None, 'value1',
                'value4', 'value4', None])

    def test_linsert(self):
        self.values.append(self.database.command_rpush('key', 'value1'))
        self.values.append(self.database.command_rpush('key', 'value2'))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_rpush('key', 'value4'))
        self.values.append(self.database.command_linsert('key', 'after',
                    'value2', 'value2.5'))
        self.values.append(self.database.command_linsert('key', 'after',
                    'value2.1', 'value2.5'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.assertEqual(self.values, [None, None, None, None, 5, -1,
                ['value1', 'value2', 'value2.5', 'value3', 'value4']])
        self.values = []
        self.values.append(self.database.command_linsert('key', 'after',
                    'value1', 'value1.5'))
        self.values.append(self.database.command_lrange('key', 0, 2))
        self.assertEqual(self.values, [6, ['value1', 'value1.5', 'value2']])

    def test_llen(self):
        self.values.append(self.database.command_llen('key'))
        self.values.append(self.database.command_rpush('key', 'value1'))
        self.values.append(self.database.command_llen('key'))
        self.values.append(self.database.command_rpush('key', 'value2'))
        self.values.append(self.database.command_llen('key'))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_llen('key'))
        self.values.append(self.database.command_rpush('key', 'value4'))
        self.values.append(self.database.command_llen('key'))
        self.assertEqual(self.values, [0, None, 1, None, 2, None, 3, None, 4])

    def test_lpop(self):
        self.values.append(self.database.command_lpop('key'))
        self.values.append(self.database.command_rpush('key', 'value1'))
        self.values.append(self.database.command_rpush('key', 'value2'))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_lpop('key'))
        self.values.append(self.database.command_lpop('key'))
        self.values.append(self.database.command_lpop('key'))
        self.values.append(self.database.command_type('key'))
        self.assertEqual(self.values, [None, None, None, None, 'value1',
                'value2', 'value3', 'none'])

    def test_rpop(self):
        self.values.append(self.database.command_rpop('key'))
        self.values.append(self.database.command_rpush('key', 'value1'))
        self.values.append(self.database.command_rpush('key', 'value2'))
        self.values.append(self.database.command_rpush('key', 'value3'))
        self.values.append(self.database.command_rpop('key'))
        self.values.append(self.database.command_rpop('key'))
        self.values.append(self.database.command_rpop('key'))
        self.values.append(self.database.command_type('key'))
        self.assertEqual(self.values, [None, None, None, None, 'value3',
                'value2', 'value1', 'none'])

    def test_rpoplpush(self):
        self.values.append(self.database.command_rpush('key1', 'value1'))
        self.values.append(self.database.command_rpush('key1', 'value2'))
        self.values.append(self.database.command_rpush('key1', 'value3'))
        self.values.append(self.database.command_rpoplpush('key1', 'key2'))
        self.values.append(self.database.command_rpoplpush('key1', 'key2'))
        self.values.append(self.database.command_rpoplpush('key1', 'key2'))
        self.values.append(self.database.command_rpoplpush('key1', 'key2'))
        self.values.append(self.database.command_type('key1'))
        self.values.append(self.database.command_lrange('key2', 0, -1))
        self.assertEqual(self.values, [None, None, None, 'value3',
                'value2', 'value1', None, 'none',
                ['value1', 'value2', 'value3']
                ])

    def test_rpushx(self):
        self.values.append(self.database.command_rpushx('key', 'value1'))
        self.values.append(self.database.command_rpush('key', 'value0'))
        self.values.append(self.database.command_rpushx('key', 'value1'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.assertEqual(self.values, [0, None, 2, ['value0', 'value1']])

    def test_lpushx(self):
        self.values.append(self.database.command_lpushx('key', 'value1'))
        self.values.append(self.database.command_lpush('key', 'value0'))
        self.values.append(self.database.command_lpushx('key', 'value1'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        self.assertEqual(self.values, [0, None, 2, ['value1', 'value0']])

    def test_lrem(self):
        values = ['a', 'b', 'c', 'd', 'a', 'b', 'c', 'd', 'a', 'b']
        for v in values:
            self.database.command_rpush('key', v)
        self.values.append(self.database.command_lrem('key', '2', 'a'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        values.remove('a')
        values.remove('a')
        self.assertEqual(self.values, [2, values])
        self.values = []
        self.values.append(self.database.command_lrem('key', '-1', 'b'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        values = values[:-1]
        self.assertEqual(self.values, [1, values])
        self.values = []
        self.values.append(self.database.command_lrem('key', '5', 'c'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        values.remove('c')
        values.remove('c')
        self.assertEqual(self.values, [2, values])
        self.values = []
        self.values.append(self.database.command_lrem('key', '0', 'd'))
        self.values.append(self.database.command_lrange('key', 0, -1))
        values.remove('d')
        values.remove('d')
        self.assertEqual(self.values, [2, values])
