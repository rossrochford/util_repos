import codecs
import datetime
import pickle

import msgpack


EXPECTED_TYPES_FOR_PICKLE = [
    'datetime', 'ObjectId'
]

#pickled = codecs.encode(pickle.dumps(obj), "base64").decode()
#unpickled = pickle.loads(codecs.decode(pickled.encode(), "base64"))


# todo: evaluate this: https://github.com/explosion/srsly
#  and this: https://github.com/capnproto/pycapnp


def pickle_encode(obj, add_prefix=False, string=True):
    data = codecs.encode(pickle.dumps(obj), "base64")
    if string:
        data = data.decode()
    if add_prefix:
        prefix = 'pickle:' if string else b'pickle:'
        data = prefix + data

    return data


def pickle_decode(data):
    if type(data) is str:
        data = data.encode()
    if data.startswith(b'pickle:'):
        data = data[6:]
    return pickle.loads(codecs.decode(data, "base64"))


def _pickle_nonprimitive_objects(obj, is_top_level):
    obj_type = type(obj)
    if obj_type in (str, int, float, bool) or obj is None:
        return obj
    if obj_type is tuple:  # convert to list
        obj = [item for item in obj]
    if obj_type is list:
        return [_pickle_nonprimitive_objects(item, False) for item in obj]
    if obj_type is dict:
        return {
            k: _pickle_nonprimitive_objects(v, False) for (k, v) in obj.items()
        }
    if obj_type.__name__ not in EXPECTED_TYPES_FOR_PICKLE:
        print(f"warning: pickling type: {obj_type}")

    get_string = not is_top_level
    return pickle_encode(obj, True, get_string)




def _unpickle_nonprimitive_objects(unpacked_object):
    obj_type = type(unpacked_object)
    if obj_type is str and unpacked_object.startswith('pickle:'):
        unpacked_object = unpacked_object[6:]
        return pickle_decode(unpacked_object)

    if obj_type in (int, float, str, bool) or unpacked_object is None:
        return unpacked_object

    if obj_type is list:
        return [
            _unpickle_nonprimitive_objects(item) for item in unpacked_object
        ]
    if obj_type is dict:
        return {
            k: _unpickle_nonprimitive_objects(v) for (k, v) in unpacked_object.items()
        }
    import pdb; pdb.set_trace()
    return None


def _msgpack_encode(obj):

    obj2 = _pickle_nonprimitive_objects(obj, True)

    if type(obj2) is bytes and obj2.startswith(b'pickle:'):
        return obj2

    try:
        return msgpack.packb(obj2, use_bin_type=True)
    except TypeError:
        return pickle_encode(obj, True, False)


def _msgpack_decode(data):
    if data.startswith(b'pickle:'):
        return pickle_decode(data)

    if type(data) is str:
        data = data.encode()

    unpacked_object = msgpack.unpackb(data, raw=False)

    return _unpickle_nonprimitive_objects(unpacked_object)


def msgpack_decode(data):
    try:
        return True, _msgpack_decode(data)
    except:
        print('error: failed to deserialize bytes')
        return False, None


def msgpack_encode(obj):
    try:
        return True, _msgpack_encode(obj)
    except:
        print('error: failed to serialize msg')
        return False, None



'''
>>> import msgpack
>>> msgpack.packb([1, 2, 3], use_bin_type=True)
'\x93\x01\x02\x03'
>>> msgpack.unpackb(_, raw=False)
'''


def main():
    import pytz
    obj1 = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    obj2 = [98234, 23423.2, None, {'obj1': obj1}]
    obj3 = {'blah': [{'obj2': obj2}, None]}

    for obj in [obj1, obj2, obj3]:
        res = msgpack_encode(obj)
        unpacked = msgpack_decode(res)
        print(obj)
        print(unpacked)
        print("\n\n")


if __name__ == '__main__':
    main()
