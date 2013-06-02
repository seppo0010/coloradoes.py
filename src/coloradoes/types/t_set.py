import random
import struct
import time

from ..errors import *

TYPE = 'T'
STRUCT_SET = '!ici'
STRUCT_SET_ELEMENT_PREFIX = '!icic'
STRUCT_SET_VALUE = '!icici'
STRUCT_KEY_SET_VALUE = '!i'

def _set_key(db, id, index=None):
    if index is None:
        return struct.pack(STRUCT_SET, db.database, TYPE, id)
    return struct.pack(STRUCT_SET_VALUE, db.database, TYPE, id, 'I', index)

def _set_element_key(db, id, element):
    return struct.pack(STRUCT_SET_ELEMENT_PREFIX, db.database, TYPE, id,
            'V') + element

def _get_info(db, id):
    if id is None:
        return None
    data = db.get(_set_key(db, id))
    if not data:
        return None
    cardinality, = struct.unpack(STRUCT_KEY_SET_VALUE, data)
    return {
        'cardinality': cardinality,
    }

def _set_info(db, id, cardinality):
    if id is None:
        return None
    assert cardinality > 0
    db.set(_set_key(db, id), struct.pack(STRUCT_KEY_SET_VALUE, cardinality))

def _add(db, id, position, value):
    db.set(_set_key(db, id, position), value)
    db.set(_set_element_key(db, id, value), position)

def _get(db, id, position):
    return db.get(_set_key(db, id, position))

def _position(db, id, value):
    pos = db.get(_set_element_key(db, id, value))
    if pos is None:
        return None
    return int(pos)

def _contains(db, id, value):
    return _position(db, id, value) is not None

def command_sadd(db, key, *args, **kwargs):
    id = kwargs.pop('id', None)
    if id is None:
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
        _add(db, id, cardinality, member)
        cardinality += 1
        added += 1

    if added != 0:
        _set_info(db, id, cardinality)
    return added

def command_smembers(db, key, id=None):
    if id is None:
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

def command_srandmember(db, key, _count=1):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    info = _get_info(db, id)
    cardinality = info['cardinality']
    count = int(_count)
    if count == 1:
        return _get(db, id, random.randint(0, cardinality - 1))
    elif count >= cardinality:
        return [_get(db, id, i) for i in range(0, cardinality)]
    return [_get(db, id, i) for i in random.sample(xrange(cardinality), count)]

def command_spop(db, key, _count=1):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    info = _get_info(db, id)
    cardinality = info['cardinality']
    if cardinality == 1:
        pos = 0
    else:
        pos = random.randint(0, cardinality - 1)
    value = _get(db, id, pos)
    if pos < cardinality - 1: # not the last element
        db.rename(_set_key(db, id, cardinality - 1), _set_key(db, id, pos))
        db.set(_set_element_key(db, id, value), pos)
    else:
        db.delete(_set_element_key(db, id, value))
        db.delete(_set_key(db, id, pos))

    if cardinality == 1:
        db.delete_key(key, id=id, type=TYPE)
    else:
        _set_info(db, id, cardinality - 1)
    return value

def command_srem(db, key, *args, **kwargs):
    id = kwargs.pop('id', None)
    type = kwargs.pop('type', None)
    if id is None:
        id, type = db.get_key(key)[:2]
        if type is None:
            return 0
        elif type != TYPE:
            raise ValueError(WRONG_TYPE)

    info = _get_info(db, id)
    cardinality = info['cardinality']

    found = 0
    for member in args:
        element_key = _set_element_key(db, id, member)
        pos = db.get(element_key)
        if pos is not None:
            found += 1
            db.delete(element_key)
            db.rename(_set_key(db, id, cardinality - found),
                    _set_key(db, id, int(pos)))
    if found > 0:
        _set_info(db, id, cardinality - found)
    return found

def command_smove(db, source, target, member):
    source_id, source_type = db.get_key(source)[:2]
    if source_id is None:
        return 0
    elif source_type != TYPE:
        raise ValueError(WRONG_TYPE)

    if not _contains(db, source_id, member):
        return 0

    target_id, target_type = db.get_key(target)[:2]
    if target_type not in (TYPE, None):
        raise ValueError(WRONG_TYPE)

    command_srem(db, source, member, id=source_id)
    command_sadd(db, target, member, id=target_id)
    return 1

def _fetch_keys_data(db, *args, **kwargs):
    include_empty = kwargs.get('include_empty', False)
    keys = []
    for key in args:
        id, type = db.get_key(key)[:2]
        if type is None:
            if include_empty:
                keys.append(key, None, None, 0)
            continue
        if type != TYPE:
            raise ValueError(WRONG_TYPE)
        info = _get_info(db, id)
        keys.append((key, id, type, info['cardinality']))
    return keys

def command_sunion(db, *args):
    retval = set()
    for key in args:
        retval.update(command_smembers(db, key))
    return list(retval)

def command_sunionstore(db, destination, *args):
    db.command_del(destination)
    keys = _fetch_keys_data(db, *args)

    destination_id = None
    for (key, id, type, cardinality) in keys:
        for i in range(0, cardinality):
            command_sadd(db, destination, _get(db, id, i), id=destination_id)
            # was it created by this SADD call?
            if destination_id is None:
                destination_id  = db.get_key(destination)[0]

def _is_inter(db, value, keys):
    for (key, _id, type, _) in keys:
        if not _contains(db, _id, value):
            return False
    return True

def _sinter(db, *args, **kwargs):
    destination = kwargs.get('destination', None)
    keys = _fetch_keys_data(db, *args, include_empty=True)

    if destination is None:
        retval = []
    else:
        retval = 0

    if len(keys) == 0:
        return retval

    # Sort sets from the smallest to largest, this will improve our
    # algorithm's performance
    keys = sorted(keys, cmp=lambda x,y: cmp(x[3], y[3]))
    (_, id, _, cardinality) = keys.pop(0)
    destination_id = None

    for i in range(0, cardinality):
        value = _get(db, id, i)
        if _is_inter(db, value, keys):
            if destination is None:
                retval.append(value)
            else:
                retval += command_sadd(db, destination, value,
                        id=destination_id)
                if destination_id is None:
                    destination_id  = db.get_key(destination)[0]
    return retval

def command_sinter(db, *args):
    return _sinter(db, *args)

def command_sinterstore(db, destination, *args):
    db.command_del(destination)
    return _sinter(db, *args, destination=destination)

def _sdiff(db, key, *args, **kwargs):
    destination = kwargs.get('destination', None)
    if destination is None:
        retval = []
    else:
        retval = 0

    id, type = db.get_key(key)[:2]
    if type is None:
        return retval
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    info = _get_info(db, id)
    cardinality = info['cardinality']

    keys = _fetch_keys_data(db, *args, include_empty=False)
    destination_id = None
    for i in range(0, cardinality):
        value = _get(db, id, i)
        contained = False
        for (key, _id, type, _) in keys:
            if _contains(db, _id, value):
                contained = True
                break
        if not contained:
            if destination is None:
                retval.append(value)
            else:
                retval += command_sadd(db, destination, value,
                        id=destination_id)
                if destination_id is None:
                    destination_id = db.get_key(destination)[0]
    return retval

def command_sdiff(db, key, *args):
    return _sdiff(db, key, *args)

def command_sdiffstore(db, destination, key, *args):
    db.command_del(destination)
    return _sdiff(db, key, *args, destination=destination)
