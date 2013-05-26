import struct
import time

SYNTAX_ERROR = 'syntax error'
SINGLE_SOURCE_KEY = '{} must be called with a single source key.'
WRONG_TYPE = 'Operation against a key holding the wrong kind of value'
WRONG_NUMBER_OF_ARGUMENTS = 'wrong number of arguments for {}'
INVALID_DB_INDEX = 'invalid DB index'


class Redislit3(object):
    def __init__(self, storage=None):
        if storage is None:
            raise ValueError('A storage is required')
        super(Redislit3, self).__init__()
        self.storage = storage
        self.database = 0

    def set_database(self, database):
        self.database = database

    def get_id(self):
        return self.storage.increment_by(struct.pack('!i', self.database) +
                'id', 1)

    def set_key(self, key, type, expire=None):
        self.command_del(key)
        id = self.get_id()
        k = struct.pack('!i', self.database) + 'K' + key
        if expire is None:
            self.storage.set(k, struct.pack('!ic', id, type))
        else:
            self.storage.set(k, struct.pack('!icd', id, type, expire))
        return id

    def delete_key(self, key, id=None, type=None):
        if id is None or type is None:
            id, type, expire = self.get_key(key, delete_expire=False)
        self.storage.delete(struct.pack('!i', self.database) + 'K' + key)
        # Do any type cleanup here
        self.storage.delete(struct.pack('!ici', self.database, type, id))

    def get_key(self, key, delete_expire=True):
        data = self.storage.get(struct.pack('!i', self.database) + 'K' + key)
        id, type, expire = None, None, None
        if data is not None:
            if len(data) == 13:
                id, type, expire = struct.unpack('!icd', data)
            elif len(data) == 5:
                expire = None
                id, type = struct.unpack('!ic', data)
            else:
                raise ValueError('Key has an invalid length')

            if delete_expire is True and (expire is not None and
                    expire < time.time()):
                id, type, expire = None, None, None
                self.delete_key(key=key, id=id, type=type)

        return id, type, expire

    def str_key(self, id):
        return struct.pack('!ici', self.database, 'S', id)

    def command_select(self, database):
        d = int(database)
        if d >= 0 and d < 17:
            self.set_database(d)
            return True
        else:
            raise ValueError(INVALID_DB_INDEX)

    def command_setex(self, key, ttl, value):
        return self.command_set(key, value, expire=time.time() + float(ttl))

    def command_psetex(self, key, ttl, value):
        return self.command_set(key, value, expire=time.time() +
                (float(ttl) / 1000.0))

    def command_ttl(self, key):
        return self.get_key(key)[2]

    def command_setnx(self, key, value):
        return self.command_set(key, value, replace=False)

    def command_set(self, key, value, expire=None, replace=True):
        old_id, old_type, old_expire = self.get_key(key)
        if old_id is not None:
            if not replace:
                return False
            self.delete_key(key, id=old_id, type=old_type)
        id = self.set_key(key, 'S', expire=expire)
        self.storage.set(self.str_key(id), value)
        return True

    def command_get(self, key):
        id, type, expire = self.get_key(key)
        if id is None:
            return None
        if type != 'S':
            raise ValueError(WRONG_TYPE)
        return self.storage.get(self.str_key(id))

    def command_del(self, *args):
        deleted = 0
        for key in args:
            id, type, expire = self.get_key(key)
            if id is not None:
                self.delete_key(key, id=id, type=type)
                deleted += 1
        return deleted

    def command_append(self, key, value):
        id, type, expire = self.get_key(key)
        if id is None:
            return self.command_set(key, value)
        if type != 'S':
            raise ValueError(WRONG_TYPE)
        str_key = self.str_key(id)
        old_value = self.storage.get(str_key)
        new_value = old_value + value
        self.storage.set(str_key, new_value)
        return len(new_value)

    def command_getrange(self, key, start=0, end=-1):
        id, type, expire = self.get_key(key)
        if id is None:
            return ''
        if type != 'S':
            raise ValueError(WRONG_TYPE)
        if end == -1:
            end = None
        else:
            end += 1
        return self.storage.get(self.str_key(id))[start:end]

    def command_getset(self, key, value):
        old_value = self.command_get(key)
        self.command_set(key, value)
        return old_value

    def command_incrby(self, key, increment):
        id, type, expire = self.get_key(key)
        if id is None:
            self.command_set(key, int(increment))
            return str(increment)
        if type != 'S':
            raise ValueError(WRONG_TYPE)
        return str(self.storage.increment_by(self.str_key(id),
                    int(increment)))

    def command_incr(self, key):
        return self.command_incrby(key, 1)

    def command_decr(self, key):
        return self.command_incrby(key, -1)

    def command_decrby(self, key, decrement):
        return self.command_incrby(key, -int(decrement))

    def command_incrbyfloat(self, key, increment):
        id, type, expire = self.get_key(key)
        if id is None:
            self.command_set(key, float(increment))
            return str(increment)
        if type != 'S':
            raise ValueError(WRONG_TYPE)
        return str(self.storage.increment_by(self.str_key(id),
                    float(increment)))

    def command_strlen(self, key):
        value = self.command_get(key)
        if value is None:
            return 0
        return len(value)

    def command_mget(self, *args):
        response = []
        for key in args:
            response.append(self.command_get(key))
        return response

    def command_mset(self, *args, **kwargs):
        if len(args) % 2 == 1:
            raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('MSET'))
        replace = kwargs.get('replace', True)
        arguments = list(args)  # copy
        response = 0
        while len(arguments) > 0:
            key = arguments.pop(0)
            value = arguments.pop(0)
            response += int(self.command_set(key, value, replace=replace))
        return response

    def command_msetnx(self, *args):
        if len(args) % 2 == 1:
            raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('MSETNX'))
        return self.command_mset(*args, replace=False)

    def command_bitcount(self, key, start=None, end=None):
        if start is not None and end is None:
            raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('BITCOUNT'))
        if start is None:
            content = self.command_get(key)
        else:
            content = self.command_getrange(key, start, end)

        if not content:
            return 0

        total = 0
        for byte in content:
            total += bin(ord(byte)).count('1')
        return total

    def command_bitop(self, operation, destkey, *keys):
        op = operation.upper()
        if op == 'NOT':
            if len(keys) != 1:
                raise ValueError(SINGLE_SOURCE_KEY.format('BITOP NOT'))
        elif op not in ('AND', 'OR', 'XOR'):
            raise ValueError(SYNTAX_ERROR)

        values = [self.command_get(key) for key in keys]
        length = max((lambda x: len(x) if x else 0)(x) for x in values)
        result = None
        for value in values:
            v = value.ljust(length, '\0')
            if result is None:
                result = v
            else:
                new_result = ''
                for i in range(0, length):
                    if op == 'AND':
                        new_result += chr(ord(result[i]) & ord(v[i]))
                    elif op == 'OR':
                        new_result += chr(ord(result[i]) | ord(v[i]))
                    elif op == 'XOR':
                        new_result += chr(ord(result[i]) ^ ord(v[i]))
                result = new_result
        if op == 'NOT':
            result = ''.join((chr((~ord(c) + 256) % 256) for c in result))
        self.command_set(destkey, result)

        return length
