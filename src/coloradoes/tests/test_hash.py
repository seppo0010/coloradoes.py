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
