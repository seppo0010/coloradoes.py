import unittest

from .. import Coloradoes
from ..storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.database = Coloradoes(Storage())
        self.values = []

    def test_zadd_inspect(self):
        self.values.append(self.database.command_zadd('key', '100.1', '100.1',
                    '100.1', '100.1a', '100.2', '100.2'))
        self.values.append(self.database.command_zrange('key', '-inf', '+inf',
                'WITHSCORES'))
        self.assertEquals(self.values, [3,
                ['100.1', '100.1', '100.1a', '100.1', '100.2', '100.2']])
