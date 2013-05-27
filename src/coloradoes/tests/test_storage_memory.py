import unittest
from ..storage.memory import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        super(TestStorage, self).setUp()
        self.storage = Storage()
        self.values = []

    def test_increment(self):
        self.values.append(self.storage.increment_by('key1', 1))
        self.values.append(self.storage.increment_by('key1', 1))
        self.values.append(self.storage.increment_by('key2', 1))
        self.assertEqual(self.values, [1, 2, 1])

    def test_set(self):
        self.values.append(self.storage.get('key1'))
        self.values.append(self.storage.set('key1', 'value'))
        self.values.append(self.storage.get('key1'))
        self.assertEqual(self.values, [None, None, 'value'])

    def test_set_increment(self):
        self.values.append(self.storage.set('key1', '123'))
        self.values.append(self.storage.increment_by('key1', 1))
        self.assertEqual(self.values, [None, 124])

    def test_set_increment_invalid(self):
        self.storage.set('key1', 'test')
        with self.assertRaises(ValueError):
            self.storage.increment_by('key1', 1)

    def test_exists(self):
        self.values.append(self.storage.exists('key1'))
        self.values.append(self.storage.set('key1', 'asd'))
        self.values.append(self.storage.exists('key1'))
        self.assertEqual(self.values, [False, None, True])

    def test_delete(self):
        self.values.append(self.storage.set('key1', 'asd'))
        self.values.append(self.storage.delete('key1'))
        self.values.append(self.storage.delete('key1'))
        self.assertEqual(self.values, [None, 1, 0])
