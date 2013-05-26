import time
import unittest

from redislit3 import Redislit3
from redislit3.storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Redislit3(Storage())
        self.values = []

    def test_set_get_different_db(self):
        self.values.append(self.database.command_set('key', 'value'))
        self.values.append(self.database.command_select('1'))
        self.values.append(self.database.command_get('key'))
        self.values.append(self.database.command_set('key', 'value1'))
        self.values.append(self.database.command_get('key'))
        self.values.append(self.database.command_select('0'))
        self.values.append(self.database.command_get('key'))
        self.assertEqual(self.values, [True, True, None, True, 'value1', True,
                'value'])
