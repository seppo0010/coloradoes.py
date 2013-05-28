import struct

from ..errors import *

TYPE = 'L'
STRUCT_KEY_LIST = '!ici'
STRUCT_KEY_LIST_VALUE = '!ii'
STRUCT_LIST = '!icii'

def _key(db, id, index=None):
    if index is None:
        return struct.pack(STRUCT_KEY_LIST, db.database, TYPE, id)
    return struct.pack(STRUCT_LIST, db.database, TYPE, id, index)

def _set_info(db, id, left, right):
    db.storage.set(_key(db, id), struct.pack('!ii', left, right))

def _get_info(db, id):
    if id is None:
        return None
    data = db.storage.get(_key(db, id))
    if not data:
        return None
    left, right = struct.unpack(STRUCT_KEY_LIST_VALUE, data)
    return {
        'left': left,
        'right': right,
    }

def _get_pos(pos, info):
    if pos >= 0:
        return info['left'] + pos
    else:
        ret = info['right'] + pos + 1
        if ret < info['left']:
            ret = info['left']
        return ret

def _push(db, key, value, pos, create=True):
    id, type = db.get_key(key)[:2]
    if not create and type is None:
        return False

    if type not in (None, TYPE):
        raise ValueError(WRONG_TYPE)

    info = _get_info(db, id)
    if info is None:
        id = db.set_key(key, 'L')
        db.storage.set(_key(db, id, 0), value)
        _set_info(db, id, 0, 0)
    else:
        if pos == -1:
            left = info['left'] - 1
            right = info['right']
            db.storage.set(_key(db, id, left), value)
        else:
            left = info['left']
            right = info['right'] + 1
            db.storage.set(_key(db, id, right), value)
        _set_info(db, id, left, right)
    return True

def _pop(db, key_name, pos):
    id, type = db.get_key(key_name)[:2]
    if type is None:
        return None
    if type != TYPE:
        raise ValueError(WRONG_TYPE)
    info = _get_info(db, id)
    left, right = info['left'], info['right']
    if pos == -1:
        index = left
        left += 1
    else:
        index = right
        right -= 1
    key = _key(db, id, index)
    value = db.storage.get(key)
    db.storage.delete(key)
    if right < left:
        db.delete_key(key_name, id=id, type=type)
    else:
        _set_info(db, id, left, right)
    return value

def command_lpush(db, key, value):
    _push(db, key, value, -1)

def command_rpush(db, key, value):
    _push(db, key, value, 1)

def command_lrange(db, key, _start, _end):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    if type != TYPE:
        raise ValueError(WRONG_TYPE)
    info = _get_info(db, id)

    start, end = int(_start), int(_end)
    # Ends in a position before the first one?
    if end < 0 and info['right'] - info['left'] < - 1 - end: return []
    left, right = _get_pos(start, info), _get_pos(end, info)

    return [db.storage.get(_key(db, id, i)) for i in
            range(left, right + 1)]

def command_lindex(db, key, _index):
    id, type = db.get_key(key)[:2]
    if type is None:
        return None
    if type != TYPE:
        raise ValueError(WRONG_TYPE)
    info = _get_info(db, id)

    index = int(_index)
    if index < 0 and info['right'] - info['left'] < - 1 - index: return None
    pos = _get_pos(index, info)
    return db.storage.get(_key(db, id, pos))

def command_linsert(db, key, position, pivot, value):
    id, type = db.get_key(key)[:2]
    if type is None:
        return None
    if type != TYPE:
        raise ValueError(WRONG_TYPE)

    if position.upper() not in ('BEFORE', 'AFTER'):
        raise ValueError(SYNTAX_ERROR)
    pos = int(position.upper() == 'AFTER')
    info = _get_info(db, id)

    for i in range(info['left'], info['right'] + 1):
        if db.storage.get(_key(db, id, i)) == pivot:
            for j in range(info['left'], i + pos):
                db.storage.rename(_key(db, id, j), _key(db, id, j - 1))
            db.storage.set(_key(db, id, i + pos - 1), value)
            _set_info(db, id, info['left'] - 1, info['right'])
            return info['right'] - info['left'] + 2
    return -1

def command_llen(db, key):
    id, type = db.get_key(key)[:2]
    if type is None:
        return 0
    if type != TYPE:
        raise ValueError(WRONG_TYPE)
    info = _get_info(db, id)
    return info['right'] - info['left'] + 1

def command_lpop(db, key):
    return _pop(db, key, -1)

def command_rpop(db, key):
    return _pop(db, key, 1)
