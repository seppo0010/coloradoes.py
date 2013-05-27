import struct
import time

from .types import string
from .errors import *

class Redislit3(object):
    STRUCT_KEY = '!ic'
    STRUCT_KEY_VALUE = '!icd'
    STRUCT_ID = '!i'

    def __init__(self, storage=None):
        if storage is None:
            raise ValueError('A storage is required')
        super(Redislit3, self).__init__()
        self.storage = storage
        self.database = 0

    def set_database(self, database):
        self.database = database

    def get_id(self):
        return self.storage.increment_by(struct.pack(self.STRUCT_ID,
                    self.database) + 'id', 1)

    def set_key(self, key, type, expire=None):
        self.command_del(key)
        id = self.get_id()
        k = struct.pack(self.STRUCT_KEY, self.database, 'K') + key
        self.storage.set(k, struct.pack(self.STRUCT_KEY_VALUE, id, type,
                    expire or 0))
        return id

    def delete_key(self, key, id=None, type=None):
        if id is None or type is None:
            id, type = self.get_key(key, delete_expire=False)[:2]
        self.storage.delete(struct.pack(self.STRUCT_KEY, self.database, 'K') +
                key)
        # Do any type cleanup here
        # TODO: call type-specific clean up method
        self.storage.delete(struct.pack('!ici', self.database, type, id))

    def get_key(self, key, delete_expire=True):
        data = self.storage.get(struct.pack(self.STRUCT_KEY, self.database,
                'K') + key)
        id, type, expire = None, None, None
        if data is not None:
            id, type, expire = struct.unpack(self.STRUCT_KEY_VALUE, data)
            if expire == 0:
                expire = None

            if delete_expire is True and (expire is not None and
                    expire < time.time()):
                id, type, expire = None, None, None
                self.delete_key(key=key, id=id, type=type)

        return id, type, expire

    def command_select(self, database):
        d = int(database)
        if d >= 0 and d < 17:
            self.set_database(d)
            return True
        else:
            raise ValueError(INVALID_DB_INDEX)

    def __getattr__(self, attrName):
        if attrName not in self.__dict__:
            if attrName.startswith('command_'):
                for t in (string,):
                    if hasattr(t, attrName):
                        def func(*args, **kwargs):
                            return getattr(t, attrName)(self, *args, **kwargs)
                        return func
            raise AttributeError()
        return self.__dict__[attrName]
