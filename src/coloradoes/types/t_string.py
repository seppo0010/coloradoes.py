import struct
import time

from ..errors import *

STRUCT_STRING = '!ici'

def _str_key(db, id):
    return struct.pack(STRUCT_STRING, db.database, 'S', id)

def command_setex(db, key, ttl, value):
    return command_set(db, key, value, expire=time.time() + float(ttl))

def command_psetex(db, key, ttl, value):
    return command_set(db, key, value, expire=time.time() +
            (float(ttl) / 1000.0))

def command_ttl(db, key):
    return db.get_key(key)[2]

def command_setnx(db, key, value):
    return command_set(db, key, value, replace=False)

def command_set(db, key, value, expire=None, replace=True):
    old_id, old_type = db.get_key(key)[:2]
    if old_id is not None:
        if not replace:
            return False
        db.delete_key(key, id=old_id, type=old_type)
    id = db.set_key(key, 'S', expire=expire)
    db.set(_str_key(db, id), value)
    return True

def command_get(db, key):
    id, type = db.get_key(key)[:2]
    if id is None:
        return None
    if type != 'S':
        raise ValueError(WRONG_TYPE)
    return db.get(_str_key(db, id))

def command_del(db, *args):
    deleted = 0
    for key in args:
        id, type = db.get_key(key)[:2]
        if id is not None:
            db.delete_key(key, id=id, type=type)
            deleted += 1
    return deleted

def command_append(db, key, value):
    id, type = db.get_key(key)[:2]
    if id is None:
        return command_set(db, key, value)
    if type != 'S':
        raise ValueError(WRONG_TYPE)
    str_key = _str_key(db, id)
    old_value = db.get(db, str_key)
    new_value = old_value + value
    db.set(str_key, new_value)
    return len(new_value)

def command_getrange(db, key, start=0, end=-1):
    id, type = db.get_key(key)[:2]
    if id is None:
        return ''
    if type != 'S':
        raise ValueError(WRONG_TYPE)
    if end == -1:
        end = None
    else:
        end += 1
    return db.get(_str_key(db, id))[start:end]

def command_setrange(db, key, start, value):
    id, type = db.get_key(key)[:2]
    if id is None:
        return ''
    if type != 'S':
        raise ValueError(WRONG_TYPE)
    str_key = _str_key(db, id)
    old_value = db.get(str_key)
    new_value = old_value[:start] + value + old_value[start + len(value):]
    db.set(str_key, new_value)
    return len(new_value)

def command_getset(db, key, value):
    old_value = command_get(db, key)
    command_set(db, key, value)
    return old_value

def command_incrby(db, key, increment):
    id, type = db.get_key(key)[:2]
    if id is None:
        command_set(db, key, int(increment))
        return str(increment)
    if type != 'S':
        raise ValueError(WRONG_TYPE)
    return str(db.increment_by(_str_key(db, id), int(increment)))

def command_incr(db, key):
    return command_incrby(db, key, 1)

def command_decr(db, key):
    return command_incrby(db, key, -1)

def command_decrby(db, key, decrement):
    return command_incrby(db, key, -int(decrement))

def command_incrbyfloat(db, key, increment):
    id, type = db.get_key(key)[:2]
    if id is None:
        command_set(db, key, float(increment))
        return str(increment)
    if type != 'S':
        raise ValueError(WRONG_TYPE)
    return str(db.increment_by(_str_key(db, id), float(increment)))

def command_strlen(db, key):
    return len(command_get(db, key) or '')

def command_mget(db, *args):
    return [command_get(db, key) for key in args]

def command_mset(db, *args, **kwargs):
    if len(args) % 2 == 1:
        raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('MSET'))
    replace = kwargs.get('replace', True)
    arguments = list(args)  # copy
    response = 0
    while len(arguments) > 0:
        key = arguments.pop(0)
        value = arguments.pop(0)
        response += int(command_set(db, key, value, replace=replace))
    return response

def command_msetnx(db, *args):
    if len(args) % 2 == 1:
        raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('MSETNX'))
    return command_mset(db, *args, replace=False)

def command_bitcount(db, key, start=None, end=None):
    if start is not None and end is None:
        raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('BITCOUNT'))
    if start is None:
        content = command_get(db, key)
    else:
        content = command_getrange(db, key, start, end)

    if not content:
        return 0

    total = 0
    for byte in content:
        total += bin(ord(byte)).count('1')
    return total

def command_bitop(db, operation, destkey, *keys):
    op = operation.upper()
    if op == 'NOT':
        if len(keys) != 1:
            raise ValueError(SINGLE_SOURCE_KEY.format('BITOP NOT'))
    elif op not in ('AND', 'OR', 'XOR'):
        raise ValueError(SYNTAX_ERROR)

    values = [command_get(db, key) for key in keys]
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
    command_set(db, destkey, result)

    return length

def command_getbit(db, key, offset):
    bitoffset = int(offset)
    value = command_get(db, key)
    byte = bitoffset >> 3
    bit = 7 - (bitoffset & 0x7)
    if len(value) <= byte:
        return 0
    return int(bool((ord(value[byte]) & (1 << bit))))

def command_setbit(db, key, offset, onoff):
    bitoffset = int(offset)
    value = command_get(db, key)
    byte = bitoffset >> 3
    bit = 7 - (bitoffset & 0x7)
    if onoff == '1':
        new_char = chr(ord(value[byte]) | (1 << bit))
    else:
        new_char = chr((ord(value[byte]) & ~(1 << bit)) % 256)
    new_value = new_char.join((value[:byte], value[byte + 1:]))
    command_set(db, key, new_value)
    return len(new_value)
