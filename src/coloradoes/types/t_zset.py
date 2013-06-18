from math import floor
import struct

from ..errors import *

TYPE = 'T'
STRUCT_ZSET_SCORE = '!d'
STRUCT_ZSET_ITEM_COUNT = '!i'
STRUCT_ZSET_VALUE = '!ici'
STRUCT_ZSET_SCORE_KEY = '!icicd'
STRUCT_ZSET_SCORE_POSITION = '!icicdi'
STRUCT_ZSET_ELEMENT = '!icic'
STRUCT_ZSET_ELEMENT_VALUE = '!di'

def _zset_key(db, id):
    return struct.pack(STRUCT_ZSET_VALUE, db.database, TYPE, id)

def _zset_key_score(db, id, score):
    return struct.pack(STRUCT_ZSET_SCORE_KEY, db.database, TYPE, id, 'S',
            score)

def _zset_key_score_position(db, id, score, position):
    return struct.pack(STRUCT_ZSET_SCORE_POSITION, db.database, TYPE, id,
            'S', score, position)

def _zset_key_score_element(db, id, element):
    return struct.pack(STRUCT_ZSET_ELEMENT, db.database, TYPE, id, 'E'
            ) + element

def _zset_key_score_element_value(score, position):
    return struct.pack(STRUCT_ZSET_ELEMENT_VALUE, score, position)

def _zset_data_find_score(data, score):
    _min, _max = 0, len(data) / 8
    while _min < _max:
        pos = int(floor((_max - _min) / 2) + _min)
        data_pos = pos * 8
        pos_score = struct.unpack(STRUCT_ZSET_SCORE,
                data[data_pos:data_pos + 8])[0]
        if score == pos_score:
            return pos, True
        elif score < pos_score:
            _max = pos - 1
        else:
            _min = pos + 1
    return _min, False

def command_zadd(db, key, *args):
    if len(args) % 2 == 1:
        raise ValueError(WRONG_NUMBER_OF_ARGUMENTS.format('ZADD'))

    id, type = db.get_key(key)[:2]
    if type not in (TYPE, None):
        raise ValueError(WRONG_TYPE)

    if id is None:
        id = db.set_key(key, TYPE)
    zset_key = _zset_key(db, id)
    if type is None:
        data = ''
    else:
        data = db.get(zset_key)

    arguments = list(args)  # copy
    retval = 0
    while len(arguments) > 0:
        score = float(arguments.pop(0))
        member = arguments.pop(0)
        score_binary = struct.pack(STRUCT_ZSET_SCORE, score)

        zset_key_score = _zset_key_score(db, id, score)
        _items_in_score = db.get(zset_key_score)

        if _items_in_score is None:
            items_in_score = 0
            pos, found = _zset_data_find_score(data, score)
            assert not found
            data = data[:pos * 8] + score_binary + data[pos * 8:]
        else:
            items_in_score, = struct.unpack(STRUCT_ZSET_ITEM_COUNT,
                    _items_in_score)

        # Remove previous score of member if exists
        retval += 1 - command_zrem(db, key, member, id=id)
        db.set(_zset_key_score_position(db, id, score, items_in_score), member)
        db.set(_zset_key_score_element(db, id, member),
                _zset_key_score_element_value(score, items_in_score))
        db.set(zset_key_score, struct.pack(STRUCT_ZSET_ITEM_COUNT,
                    items_in_score + 1))

    db.set(zset_key, data)
    return retval

def command_zrem(db, key, member, id=None):
    # TODO
    return 0

def command_zrange(db, key, start, end, withscores=''):
    id, type = db.get_key(key)[:2]
    if type is None:
        return []
    elif type != TYPE:
        raise ValueError(WRONG_TYPE)

    if start != '-inf' and end != '+inf' and float(start) >= float(end):
        return []

    withscores = withscores.upper() == 'WITHSCORES'
    data = db.get(_zset_key(db, id))

    if start == '-inf':
        start_pos = 0
    else:
        start_pos, found = _zset_data_find_score(data, float(start))

    if end == '+inf':
        end_pos = len(data) / 8
    else:
        end_pos, found = _zset_data_find_score(data, float(end))
        if found:
            end_pos += 1

    retval = []
    for i in range(start_pos, end_pos):
        print (i * 8, (i + 1) * 8)
        score = struct.unpack(STRUCT_ZSET_SCORE, data[i * 8:(i + 1) * 8])[0]
        items_in_score, = struct.unpack(STRUCT_ZSET_ITEM_COUNT,
                db.get(_zset_key_score(db, id, score)))
        for j in range(0, items_in_score):
            retval.append(db.get(_zset_key_score_position(db, id, score, j)))
            if withscores:
                retval.append(str(score))
    return retval
