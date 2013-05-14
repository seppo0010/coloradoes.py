import time
import unittest

from redislit3 import Redislit3
from redislit3.storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Redislit3(Storage())
        self.values = []

    def test_setex(self):
        now = float(time.time())
        self.values.append(self.database.command_setex('key', 1, 'value'))
        self.assertGreater(self.database.command_ttl('key'), now + 1.0)
        self.assertLess(self.database.command_ttl('key'), now + 2.0)
        self.assertEqual(self.database.command_get('key'), 'value')

    def test_psetex(self):
        now = float(time.time())
        self.values.append(self.database.command_psetex('key', '1000',
                    'value'))
        self.assertGreater(self.database.command_ttl('key'), now + 1.0)
        self.assertLess(self.database.command_ttl('key'), now + 2.0)
        self.assertEqual(self.database.command_get('key'), 'value')
