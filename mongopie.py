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

from datetime import datetime
from pymongo import Connection, ASCENDING, DESCENDING
from pymongo.cursor import Cursor
from bson.objectid import ObjectId

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
    def __init__(self, cls, cursor):
        self.cls = cls
        self.cursor = cursor
    
    def __repr__(self):
        return repr(list(self))

    def __iter__(self):
        return self

    def __getitem__(self, index):
        data = self.cursor.__getitem__(index)
        if isinstance(data, Cursor):
            return CursorWrapper(self.cls, data)
        else:
            assert isinstance(data, dict)
            return self.cls.get_from_data(data)

    def count(self, **kw):
        return self.cursor.count(**kw)

    def sort(self, *fields):
        cols = []
        for f in fields:
            if f.startswith('-'):
                cols.append((f[1:], DESCENDING))
            else:
                cols.append((f, ASCENDING))
        r = self.cursor.sort(cols)
        return CursorWrapper(self.cls, r)

    def find(self, **kwargs):
        kwargs = self.cls.filter_condition(kwargs)
        r = self.cursor.find(kwargs)
        return CursorWrapper(self.cls, r)

    def next(self):
        datadict = self.cursor.next()
        return self.cls.get_from_data(datadict)

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
        value = int(value)
        super(IntegerField, self).__set__(obj, value)

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

    def __get__(self, obj, type=None):
        objid = super(ReferenceField, self).__get__(obj, type=type)
        if objid is self.default_value:
            return self.default_value
        val = self.ref_cls.get(objid)
        return val
    
    def __set__(self, obj, value):
        if isinstance(value, self.ref_cls):
            value = ObjectId(value.id)
        super(ReferenceField, self).__set__(obj, value)

class DateTimeField(Field):
    def __init__(self, default=None, **kwargs):
        self.auto_now = kwargs.get('auto_now', False)
        super(DateTimeField, self).__init__(default=default,
                                            **kwargs)

    def __get__(self, obj, type=None):
        val = super(DateTimeField, self).__get__(obj,
                                                 type=type)
        if val is None and self.auto_now:
            val = datetime.now()
            self.__set__(obj, val)
        return val

    def __set__(self, obj, value):
        if value is not None:
            assert isinstance(value, datetime)
        super(DateTimeField, self).__set__(obj, value)

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

    @classmethod
    def initialize(cls):
        """ Initialize the necessary stuffs of a model class
        Including:
            * Touch db if not exist.
        Called in ModelMeta's __new__
        """
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

    @classmethod
    def filter_condition(cls, conditions):
        newcondition = {}
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
    def find(cls, **conditions):
        conditions = cls.filter_condition(conditions)
        col = cls.collection()
        return CursorWrapper(cls, col.find(conditions))

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
        conditions = cls.filter_condition(conditions)        
        return cls.collection().remove(conditions)

    def erase(self):
        return cls.collection().remove(_id=self.id)

    @classmethod
    def get(cls, objid, **kw):
        if objid is None:
            return None
        if isinstance(objid, basestring):
            objid = ObjectId(objid)
        assert isinstance(objid, ObjectId);
        col = cls.collection()
        kw.update({'_id': objid})        
        datadict = col.find_one(kw)
        if datadict is not None:
            obj = cls(**force_string_keys(datadict))
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
        self.id = col.save(self.get_dict())
        if new:
            self.on_created()
    
    def on_created(self):
        pass
    
    def get_dict(self):
        """ Get the dict representation of an object's fields
        """
        info_dict = {}
        for field in self.fields:
            key = field.get_key()
            value = field.get_raw(self)

            if value is not field.default_value:
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
