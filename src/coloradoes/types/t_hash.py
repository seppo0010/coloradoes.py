import random
import struct
import time

from ..errors import *

TYPE = 'H'
STRUCT_HASH = '!ici'
STRUCT_HASH_ELEMENT_PREFIX = '!icic'
STRUCT_HASH_VALUE = '!icici'
STRUCT_KEY_HASH_VALUE = '!i'

def _hash_key(db, id):
    return struct.pack(STRUCT_HASH, db.database, TYPE, id)

def _hash_index_key(db, id, index):
    return struct.pack(STRUCT_HASH_VALUE, db.database, TYPE, id, 'I', index)

def _hash_field_key(db, id, field):
    return struct.pack(STRUCT_HASH_ELEMENT_PREFIX, db.database, TYPE, id, 'V'
            ) + field

def _get_info(db, id):
    if id is None:
        return None
    data = db.get(_hash_key(db, id))
    if not data:
        return None
    cardinality, = struct.unpack(STRUCT_KEY_HASH_VALUE, data)
    return {
        'cardinality': cardinality,
    }

def _set_info(db, id, cardinality):
    if id is None:
        return None
    assert cardinality > 0
    db.set(_hash_key(db, id), struct.pack(STRUCT_KEY_HASH_VALUE, cardinality))

def _add(db, id, position, field, value):
    db.set(_hash_index_key(db, id, position), field)
    db.set(_hash_field_key(db, id, field),
            struct.pack(STRUCT_KEY_HASH_VALUE, position) + value)

def _position(db, id, field):
    if id is None:
        return None
    pos = db.get(_hash_field_key(db, id, field))
    if pos is None:
        return None
    return struct.unpack(STRUCT_KEY_HASH_VALUE, pos[:4])[0]

def _contains(db, id, field):
    return _position(db, id, field) is not None

def command_hset(db, key, field, value, id=None):
    if id is None:
        id, type = db.get_key(key)[:2]
        if type not in (None, TYPE):
            raise ValueError(WRONG_TYPE)

    if id is None:
        id = db.set_key(key, TYPE)
        position = 0
    else:
        position = _position(db, id, field)

    if position is None:
        position = _get_info(db, id)['cardinality']
    _set_info(db, id, position + 1)
    _add(db, id, position, field, value)
    return True

def command_hget(db, key, field, id=None):
    if id is None:
        id, type = db.get_key(key)[:2]
        if type is None:
            return None
        elif type != TYPE:
            raise ValueError(WRONG_TYPE)

    data = db.get(_hash_field_key(db, id, field))
    if data is None:
        return None
    return data[4:]

def command_hdel(db, key, *args, **kwargs):
    id = kwargs.get('id', None)
    if id is None:
        id, type = db.get_key(key)[:2]
        if type is None:
            return 0
        elif type != TYPE:
            raise ValueError(WRONG_TYPE)

    cardinality = _get_info(db, id)['cardinality']
    del_count = 0
    for field in args:
        position = _position(db, id, field)
        if position is not None:
            last_position = cardinality - del_count - 1
            if position == last_position:
                # is the last element
                last_field = field
                db.delete(_hash_field_key(db, id, last_field))
            else:
                # copy the last element to replace the one to delete
                last_field = db.get(_hash_index_key(db, id, last_position))
                last_value = db.get(_hash_field_key(db, id, last_field))[4:]
                _add(db, id, position, last_field, last_value)
            db.delete(_hash_index_key(db, id, last_position))
            del_count += 1
    return del_count

def command_hexists(db, key, field):
    id, type = db.get_key(key)[:2]
    if type is None:
        return False
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    return _contains(db, id, field)

def command_hgetall(db, key):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    cardinality = _get_info(db, id)['cardinality']
    retval = []
    for position in range(0, cardinality):
        field = db.get(_hash_index_key(db, id, position))
        value = db.get(_hash_field_key(db, id, field))[4:]
        retval.extend((field, value))
    return retval

def command_hincrby(db, key, field, increment):
    id, type = db.get_key(key)[:2]
    if type not in (None, TYPE):
        raise ValueError(WRONG_TYPE)

    if type is None:
        new_value = int(increment)
        id = db.set_key(key, TYPE)
        _add(db, id, 0, field, str(new_value))
    else:
        data = db.get(_hash_field_key(db, id, field))
        new_value = int(data[4:]) + int(increment)
        db.set(_hash_field_key(db, id, field), data[:4] + str(new_value))

    return new_value

def command_hincrbyfloat(db, key, field, increment):
    id, type = db.get_key(key)[:2]
    if type not in (None, TYPE):
        raise ValueError(WRONG_TYPE)

    if type is None:
        new_value = float(increment)
        id = db.set_key(key, TYPE)
        _add(db, id, 0, field, str(new_value))
    else:
        data = db.get(_hash_field_key(db, id, field))
        new_value = float(data[4:]) + float(increment)
        db.set(_hash_field_key(db, id, field), data[:4] + str(new_value))

    return new_value

def command_hkeys(db, key):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    cardinality = _get_info(db, id)['cardinality']
    retval = []
    for position in range(0, cardinality):
        field = db.get(_hash_index_key(db, id, position))
        retval.append(field)
    return retval

def command_hlen(db, key):
    id, type = db.get_key(key)[:2]
    if type is None:
        return 0
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    return _get_info(db, id)['cardinality']
