################################################
#
# mongopie.py  - Object-Mongodb mapping
#
# LICENSE
# ======
#
# mongopie is licensed under MIT license
# http://en.wikipedia.org/wiki/MIT_License
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "mongopie"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
#
# Author: Zeng Ke
# Email: superisaac.ke@gmail.com
#
################################################

from datetime import datetime
from pymongo import Connection, ASCENDING, DESCENDING
from pymongo.cursor import Cursor
from bson.objectid import ObjectId, InvalidId
from collections import defaultdict

# Simple signal hub
class SignalSlot(object):
    def __init__(self):
        self.clear()

    def connect(self, sender, handler):
        if sender is None:
            sender = 'root'
        handlers = self.handlers[sender]
        handlers.append(handler)
        return len(handlers) - 1

    def disconnect(self, sender, index):
        if sender is None:
            sender = 'root'
        self.handlers[sender][index] = None

    def send(self, sender, **kw):
        if sender is None:
            sender = 'root'
        handlers = self.handlers[sender]
        for handler in handlers:
            if handler:
                handler(sender, **kw)

    def clear(self):
        self.handlers = defaultdict(list)

class ModelSignal():
    def __init__(self):
        self.pre_update = SignalSlot()
        self.post_update = SignalSlot()
        self.pre_create = SignalSlot()
        self.post_create = SignalSlot()

modelsignal = ModelSignal()

def force_string_keys(datadict):
    return dict((k.encode('utf-8'), v)
                for k, v in datadict.iteritems())

default_db = ('localhost', 27017, 'modeltest')
def set_defaultdb(host, port, name):
    global default_db
    default_db = (host, port, name)

_conn_pool = {}
def get_server(host, port, db_name):
    if (host, port) not in _conn_pool:
        conn = Connection(host, port)
        _conn_pool[(host, port)] = conn
    return _conn_pool[(host, port)][db_name]


class CursorWrapper:
    index=None
    def __init__(self, cls, conditions=None, orders=None, index=None):
        if conditions:
            self.conditions = conditions
        else:
            self.conditions = {}

        if orders:
            self.orders = orders
        else:
            self.orders = []

        if index:
            self.index = index
        self.cls = cls

    def get_cursor(self):
        col = self.cls.collection()
        cursor = col.find(self.conditions)
        if self.orders:
            cursor = cursor.sort(self.orders)
        if self.index:
            cursor = cursor.__getitem__(self.index)

        return cursor

    def __len__(self):
        return self.get_cursor().count()

    def __nonzero__(self):
        return self.get_cursor().count() > 0

    def __repr__(self):
        return repr(list(self))

    def __iter__(self):
        def cursor_iter():
            cursor = self.get_cursor()
            for datadict in cursor:
                yield self.cls.get_from_data(datadict)
        return iter(cursor_iter())

    def paginate(self, page=1, count=20):
        if page < 1:
            page = 1
        index = slice((page - 1) * count, page * count)
        return self.__getitem__(index)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return CursorWrapper(
                self.cls,
                conditions=self.conditions,
                orders=self.orders,
                index=index)
        else:
            assert isinstance(index, (int, long))
            data = self.get_cursor().__getitem__(index)
            assert isinstance(data, dict)
            return self.cls.get_from_data(data)

    def count(self):
        return self.get_cursor().count()

    def sort(self, *fields):
        cols = self.cls.make_sort(fields)
        return CursorWrapper(self.cls,
                             conditions=self.conditions,
                             orders=self.orders + cols
                             )

    def find(self, **kwargs):
        kwargs = self.cls.filter_condition(kwargs)
        conditions = self.conditions.copy()
        conditions.update(kwargs)
        return CursorWrapper(self.cls,
                             conditions=conditions,
                             orders=self.orders)

class Field(object):
    """ Field that defines the schema of a DB
    Much like the field of relation db ORMs
    A proxy of a object's attribute
    """
    def __init__(self, default=None, **args):
        self._fieldname = None
        self.default_value = default

    def _get_fieldname(self):
        return self._fieldname
    def _set_fieldname(self, v):
        self._fieldname = v
    fieldname = property(_get_fieldname, _set_fieldname)

    def get_raw(self, obj):
        return self.__get__(obj)

    def __get__(self, obj, type=None):
        return getattr(obj, self.get_obj_key(),
                       self.default_value)

    def __set__(self, obj, value):
        if value is not None:
            setattr(obj, self.get_obj_key(), value)

    def __del__(self):
        pass

    def get_key(self):
        return self.fieldname

    def get_obj_key(self):
        return '_' + self.fieldname

class BooleanField(Field):
    def __init__(self, default=False, **kwargs):
        super(BooleanField, self).__init__(default=default,
                                           **kwargs)

    def __set__(self, obj, value):
        value = not not value
        super(BooleanField, self).__set__(obj, value)

class IntegerField(Field):
    def __init__(self, default=0, **kwargs):
        super(IntegerField, self).__init__(default=default,
                                           **kwargs)

    def __set__(self, obj, value):
        value = long(value)
        super(IntegerField, self).__set__(obj, value)

class SequenceField(IntegerField):
    def __init__(self, key, default=0, **kwargs):
        self.key = key
        super(SequenceField, self).__init__(default=default, **kwargs)

class StringField(Field):
    pass

class CollectionField(Field):
    def __get__(self, obj, type=None):
        val = super(CollectionField, self).__get__(obj, type=type)
        if val is None:
            val = self.get_default_value()
            self.__set__(obj, val)
        return val

    def get_default_value(self):
        raise NotImplemented

class ArrayField(CollectionField):
    def get_default_value(self):
        return []

class DictField(CollectionField):
    def get_default_value(self):
        return {}

class ObjectIdField(Field):
    @classmethod
    def toObjectId(cls, v):
        if v is None:
            return None
        elif isinstance(v, basestring):
            # TODO: handle invalidid exception
            return ObjectId(v)
        else:
            assert isinstance(v, ObjectId)
            return v

    def __init__(self, default=None, **kwargs):
        super(ObjectIdField, self).__init__(default=default,
                                           **kwargs)

    def __set__(self, obj, value):
        value = self.toObjectId(value)
        super(ObjectIdField, self).__set__(obj, value)

    def get_key(self):
        return '_' + self.fieldname

class ReferenceField(ObjectIdField):
    def __init__(self, ref_cls, default=None, **kwargs):
        super(ReferenceField, self).__init__(default=default,
                                           **kwargs)
        self.ref_cls = ref_cls

    def get_raw(self, obj):
        return super(ReferenceField, self).__get__(obj)

    def get_ref_class(self, obj):
        return self.ref_cls == 'self' and obj.__class__ or self.ref_cls

    def __get__(self, obj, type=None):
        objid = super(ReferenceField, self).__get__(obj, type=type)
        if objid is self.default_value:
            return self.default_value
        ref_cls = self.get_ref_class(obj)
        val = ref_cls.get(objid)
        return val

    def __set__(self, obj, value):
        ref_cls = self.get_ref_class(obj)
        if isinstance(value, ref_cls):
            value = ObjectId(value.id)
        super(ReferenceField, self).__set__(obj, value)

class DateTimeField(Field):
    def __init__(self, default=None, **kwargs):
        self.auto_now_add = kwargs.get('auto_now_add', False)
        self.auto_now = kwargs.get('auto_now', False)
        super(DateTimeField, self).__init__(default=default,
                                            **kwargs)

    def __get__(self, obj, type=None):
        val = super(DateTimeField, self).__get__(obj,
                                                 type=type)
        if self.auto_now:
            val = datetime.now()
            self.__set__(obj, val)
        elif val is None and self.auto_now_add:
            val = datetime.now()
            self.__set__(obj, val)
        return val

    def __set__(self, obj, value):
        if value is not None:
            assert isinstance(value, datetime)
        super(DateTimeField, self).__set__(obj, value)

cache_classes = set()
def clear_obj_cache():
    for cls in cache_classes:
        if cls.use_obj_cache:
            cls.obj_cache = {}

class ModelMeta(type):
    """ The meta class of Model
    Do some registering of Model classes
    """
    def __new__(meta, clsname, bases, classdict):
        cls = type.__new__(meta, clsname, bases, classdict)
        if clsname == 'Model':
            return cls
        cls.initialize()
        return cls

class Model(object):
    """ The model of couchdb
    A model defines the schema of a database using its fields
    Customed model can be defined by subclassing the Model class.
    """
    __metaclass__ = ModelMeta
    index_list = []
    use_obj_cache = True

    def __str__(self):
        """
        Only use unicode method
        """
        if hasattr(self, '__unicode__'):
            return self.__unicode__()
        return super(Model, self).__str__()

    @classmethod
    def initialize(cls):
        """ Initialize the necessary stuffs of a model class
        Including:
            * Touch db if not exist.
        Called in ModelMeta's __new__
        """
        if cls.use_obj_cache:
            cls.obj_cache = {}
        cache_classes.add(cls)

        cls.col_name = cls.__name__.lower()
        idfield = ObjectIdField()
        cls.id = idfield
        cls.fields = [idfield]
        cls.field_map = {}
        for fieldname, v in vars(cls).items():
            if isinstance(v, Field):
                v.fieldname = fieldname
                cls.fields.append(v)
                cls.field_map[fieldname] = v

    @classmethod
    def ensure_indices(cls):
        ''' It's better to use js instead of this functions'''
        col = cls.collection()
        for idx, kwargs in cls.index_list:
            col.ensure_index(idx, **kwargs)

    @classmethod
    def get_auto_incr_value(cls):
        pass

    @classmethod
    def collection(cls):
        database = getattr(cls, '__database__', default_db)
        server = get_server(*database)
        return server[cls.col_name]

    def create(cls, **kwargs):
        """ Create a new object
        """
        model_obj = cls(**kwargs)
        model_obj.save()
        return model_obj

    def get_addtime(self):
        if isinstance(self.id, ObjectId):
            return self.id.generation_time

    @classmethod
    def make_sort(cls, fields):
        cols = []
        if not fields:
            return cols
        for f in fields:
            if f.startswith('-'):
                order =  DESCENDING
                f = f[1:]
            else:
                order = ASCENDING

            if f in cls.field_map:
                f = cls.field_map[f].get_key()
            cols.append((f, order))
        return cols

    @classmethod
    def make_sort_dict(cls, fields):
        cols = {}
        if not fields:
            return cols
        for f in fields:
            if f.startswith('-'):
                f = f[1:]
                order = -1
            else:
                order = 1

            if f in cls.field_map:
                f = cls.field_map[f].get_key()
            cols[f] = 1
        return cols

    @classmethod
    def filter_condition(cls, conditions):
        newcondition = {}
        if conditions is None:
            conditions = {}
        for k, v in conditions.iteritems():
            if isinstance(v, Model):
                v = v.id
            if k in cls.field_map:
                field = cls.field_map[k]
                newcondition[field.get_key()] = v
            else:
                newcondition[k] = v
        return newcondition

    @classmethod
    def find_and_modify(cls, query=None, update=None, sort=None, upsert=False, new=False):
        """
        Atomic find and modify
        """
        if cls.use_obj_cache:
            cls.obj_cache = {}
        col = cls.collection()
        query = cls.filter_condition(query)
        sort = cls.make_sort_dict(sort)
        update = cls.filter_condition(update)
        datadict = col.find_and_modify(query=query,
                                       update=update,
                                       sort=sort,
                                       upsert=upsert, new=new)
        if datadict:
            return cls.get_from_data(datadict)

    @classmethod
    def increment_field(cls, field, value=1, **query):
        return cls.find_and_modify(
            query=query,
            update={
                '$inc': {field: value}
                })

    @classmethod
    def find_and_remove(cls, query=None, sort=None):
        """
        Atomic way to dequeue an object
        """
        if cls.use_obj_cache:
            cls.obj_cache = {}
        col = cls.collection()
        query = cls.filter_condition(query)
        sort = cls.make_sort_dict(sort)
        datadict = col.find_and_modify(query=query,
                                       sort=sort,
                                       remove=True)
        if datadict:
            return cls.get_from_data(datadict)

    @classmethod
    def find(cls, **conditions):
        conditions = cls.filter_condition(conditions)
        return CursorWrapper(cls, conditions=conditions)

    @classmethod
    def find_one(cls, **conditions):
        conditions = cls.filter_condition(conditions)
        col = cls.collection()
        datadict = col.find_one(conditions)
        if datadict:
            return cls.get_from_data(datadict)
        else:
            return datadict

    @classmethod
    def count(cls):
        return cls.collection().count()

    @classmethod
    def remove(cls, **conditions):
        if cls.use_obj_cache:
            cls.obj_cache = {}
        conditions = cls.filter_condition(conditions)
        return cls.collection().remove(conditions)

    def erase(self):
        if self.use_obj_cache:
            self.__class__.obj_cache.pop(self._id, None)
        return self.collection().remove({'_id': self._id})

    @classmethod
    def multi_get(cls, objid_list):
        """ Get multiple objects in batch mode to reduce the time
        spent on network traffic
        """
        obj_dict = {}
        for obj in cls.find(_id={'$in': objid_list}):
            obj_dict[obj._id] = obj
            if cls.use_obj_cache:
                cls.obj_cache[obj._id] = obj

        for objid in objid_list:
            yield obj_dict.get(objid)

    @classmethod
    def get(cls, objid):
        """ Get an object by objectid
        """
        if objid is None:
            return None
        if isinstance(objid, basestring):
            try:
                objid = ObjectId(objid)
            except InvalidId:
                return None
        assert isinstance(objid, ObjectId);

        if cls.use_obj_cache:
            obj =  cls.obj_cache.get(objid)
            if obj:
                return obj

        col = cls.collection()
        kw = {'_id': objid}
        datadict = col.find_one(kw)
        if datadict is not None:
            obj = cls(**force_string_keys(datadict))
            if cls.use_obj_cache:
                cls.obj_cache[objid] = obj
            return obj

    def __eq__(self, other):
        return (self.__class__ == other.__class__ and
                self.id and other.id and
                self.id == other.id)

    def __hash__(self):
        return hash(self.id)

    def save(self):
        new = self.id is None
        col = self.collection()

        if new:
            modelsignal.pre_create.send(self.__class__,
                                   instance=self)
            for field in self.fields:
                if (isinstance(field, SequenceField) and
                    not getattr(self, field.fieldname, None)):
                    setattr(self, field.fieldname, SequenceModel.get_next(field.key))
        else:
            modelsignal.pre_update.send(self.__class__, instance=self)
            if self.use_obj_cache:
                self.__class__.obj_cache.pop(self.id, None)
        self.id = col.save(self.get_dict())
        if new:
            self.on_created()
            modelsignal.post_create.send(self.__class__,
                                    instance=self)
        else:
            modelsignal.post_update.send(self.__class__,
                                    instance=self)

    def on_created(self):
        pass

    def get_dict(self):
        """ Get the dict representation of an object's fields
        """
        info_dict = {}
        for field in self.fields:
            key = field.get_key()
            value = field.get_raw(self)

            if value is not None:
                info_dict[key] = value
        return info_dict

    @classmethod
    def get_from_data(cls, datadict):
        datadict = force_string_keys(datadict)
        return cls(**datadict)

    def __init__(self, **kwargs):
        for field in self.fields:
            key = field.get_key()
            if key in kwargs:
                setattr(self,
                        field.fieldname,
                        kwargs[key])

class SequenceModel(Model):
    seq = IntegerField()
    @classmethod
    def get_next(cls, key):
        col = cls.collection()
        v = col.find_and_modify(query={'_id': key},
                                update={'$inc': {'seq': 1}},
                                upsert=True, new=True)
        if v:
            return v['seq']
        return v
