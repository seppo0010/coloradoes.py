import struct
import time

from ..errors import *

TYPE = 'T'
STRUCT_SET = '!ici'
STRUCT_SET_ELEMENT_PREFIX = '!ici'
STRUCT_SET_VALUE = '!icii'
STRUCT_KEY_SET_VALUE = '!i'

def _set_key(db, id, index=None):
    if index is None:
        return struct.pack(STRUCT_SET, db.database, TYPE, id)
    return struct.pack(STRUCT_SET_VALUE, db.database, TYPE, id, index)

def _set_element_key(db, id, element):
    return struct.pack(STRUCT_SET_ELEMENT_PREFIX, db.database, TYPE, id
            ) + element

def _get_info(db, id):
    if id is None:
        return None
    data = db.storage.get(_set_key(db, id))
    if not data:
        return None
    cardinality, = struct.unpack(STRUCT_KEY_SET_VALUE, data)
    return {
        'cardinality': cardinality,
    }

def _set_info(db, id, cardinality):
    if id is None:
        return None
    db.storage.set(_set_key(db, id), struct.pack(STRUCT_KEY_SET_VALUE,
                cardinality))

def _add(db, id, position, value):
    db.storage.set(_set_key(db, id, position), value)
    db.storage.set(_set_element_key(db, id, value), position)

def _get(db, id, position):
    return db.storage.get(_set_key(db, id, position))

def _contains(db, id, value):
    return db.storage.get(_set_element_key(db, id, value)) is not None

def command_sadd(db, key, *args):
    id, type = db.get_key(key)[:2]
    if type not in (None, TYPE):
        raise ValueError(WRONG_TYPE)

    members = set(args)

    added = 0
    info = _get_info(db, id)
    if info is None:
        id = db.set_key(key, TYPE)
        _add(db, id, 0, members.pop())
        cardinality = 1
        added += 1
    else:
        cardinality = info['cardinality']

    for member in members:
        if _contains(db, id, member):
            continue
        db.storage.set(_set_key(db, id, cardinality), member)
        cardinality += 1
        added += 1

    if added != 0:
        _set_info(db, id, cardinality)
    return added

def command_smembers(db, key):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)
    info = _get_info(db, id)

    return [_get(db, id, i) for i in range(0, info['cardinality'])]

def command_scard(db, key):
    id, type = db.get_key(key)[:2]
    if type is None:
        return 0
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)
    return _get_info(db, id)['cardinality']

def command_sismember(db, key, member):
    id, type = db.get_key(key)[:2]
    if type is None:
        return False
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)
    return _contains(db, id, member)
