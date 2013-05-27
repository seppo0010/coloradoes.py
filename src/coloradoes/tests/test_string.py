import time
import unittest

from .. import Coloradoes
from ..storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Coloradoes(Storage())
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

    def test_setrange(self):
        self.values.append(self.database.command_set('key', 'value'))
        self.values.append(self.database.command_setrange('key', 0,
                    'hello world'))
        self.values.append(self.database.command_get('key'))
        self.values.append(self.database.command_setrange('key', 6, 'bro'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [True, 11, 'hello world', 11,
                'hello brold'])

    def test_getbit_setbit(self):
        self.values.append(self.database.command_set('key', 'abc'))
        self.values.append(self.database.command_getbit('key', 21))
        self.values.append(self.database.command_getbit('key', 22))
        self.values.append(self.database.command_getbit('key', 23))
        self.values.append(self.database.command_getbit('key', 24))
        self.values.append(self.database.command_setbit('key', 22, '0'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [True, 0, 1, 1, 0, 3, 'aba'])

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

    def test_bitcount(self):
        self.values.append(self.database.command_set('key', '0'))
        self.values.append(self.database.command_bitcount('key'))
        self.values.append(self.database.command_set('key', 'hello world'))
        self.values.append(self.database.command_bitcount('key'))
        self.values.append(self.database.command_bitcount('key', 0, 5))
        self.assertEqual(self.values, [True, 2, True, 45, 22])

    def test_bitop_and(self):
        self.values.append(self.database.command_set('key1', 'abcdef'))
        self.values.append(self.database.command_set('key2', 'foobar'))
        self.values.append(self.database.command_bitop('and', 'key3', 'key1', 'key2'))
        self.values.append(self.database.command_get('key3'))
        self.assertEqual(self.values, [True, True, 6, '`bc`ab'])

    def test_bitop_or(self):
        self.values.append(self.database.command_set('key1', 'foobar'))
        self.values.append(self.database.command_set('key2', 'abcdef'))
        self.values.append(self.database.command_set('key3', 'barbaz'))
        self.values.append(self.database.command_bitop('or', 'key4', 'key1',
                    'key2', 'key3'))
        self.values.append(self.database.command_get('key4'))

    def test_bitop_xor(self):
        self.values.append(self.database.command_set('key1', 'foo'))
        self.values.append(self.database.command_set('key2', 'abcdef'))
        self.values.append(self.database.command_bitop('xor', 'key3', 'key1',
                    'key2'))
        self.values.append(self.database.command_get('key3'))
        self.assertEqual(self.values, [True, True, 6, '\a\r\x0cdef'])

    def test_bitop_not(self):
        self.values.append(self.database.command_set('key1', 'foo'))
        self.values.append(self.database.command_bitop('not', 'key2', 'key1'))
        self.values.append(self.database.command_get('key2'))
        self.assertEqual(self.values, [True, 3, '\x99\x90\x90'])
