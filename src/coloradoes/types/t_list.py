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
