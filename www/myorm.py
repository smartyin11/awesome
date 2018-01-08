import logging
logging.basicConfig(level=logging.INFO)
import asyncio
import aiomysql


def log(sql, args=()):
    '输出SQL语句用'
    logging.info('SQL: %s' % sql)


def create_args_string(num):
    '创建替换字符串'
    L = []
    for i in range(num):
        L.append('?')
    return ', '.join(L)


async def create_pool(loop, **kw):
    '创建连接池'
    global __pool
    __pool = aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        username=kw.get('username'),
        password=kw.get('password'),
        db=kw.get('db'),
        autocommit=kw.get('autocommit', True),
        charset=kw.get('charset', 'utf8'),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop)


async def select(sql, args, size=None):
    '创建查询函数'
    log(sql, args)
    async with __pool.acquire() as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args)
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs


async def execute(sql, args):
    '创建通用SQL执行函数'
    log(sql, args)
    async with __pool.acquire() as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except:
            raise
        return affected


class Model(dict, metaclass=ModelMetaClass):
    '定义ORM的通用Model'

    def __init__(self, **kw):
        super().__init__(**kw)

    def __getattribute__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError('Model object has no attribute %s' % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = self.getValue(key)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(
                    field.default) else field.default
                logging.debug('using default value for %s: %s' % (key,
                                                                  str(value)))
                self[key] = value
        return value

    @classmethod
    async def find(cls, pk):
        '通过主键查找'
        rs = await select('%s where `%s`=?' % (cls.__select__,
                                               cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        '通过给定条件查找所有对应数据'
        sql = cls.__select__
        if where:
            sql.append('WHERE')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('ORDER BY')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('LIMIT')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    async def save(self):
        '存储到数据库'
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('faild to insert record: affected rows:%s' % rows)

    async def update(self):
        '更新到数据库'
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('faild to update record: affected rows:%s' % rows)

    async def delete(self):
        '从数据库删除'
        args = [self.getValueOrDefault(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to delete record: affected rows:%s' % rows)


class ModelMetaClass(type):
    '定义Mode类构造模板'

    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        mappings = dict()  #数据列的类映射字典
        fields = []  #除主键外的数据列名列表
        primarykey = None  #主键的数据列名
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('Found mapping: %s --> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primarykey:
                        raise RuntimeError(
                            'Duplicate primary key for field: %s' % k)
                    else:
                        primarykey = k
                else:
                    fields.append(k)
        if not primarykey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        #将取得的这些数据重新注入各Model类属性中
        attrs['__table__'] = tableName
        attrs['__mappings__'] = mappings
        attrs['__fields__'] = fields
        attrs['__primary_key__'] = primarykey
        #设计SQL语句字段
        attrs['__select__'] = 'SELECT `%s`, %s FROM `%s`' % (
            primarykey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'INSERT INTO `%s` (%s,`%s`) VALUES (%s)' % (
            tableName, ', '.join(escaped_fields), primarykey,
            create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'UPDATE `%s` SET %s WHERE `%s` = ?' % (
            tableName, ', '.join(
                map(lambda f: '`%s` = ?' % (mappings.get(f).name or f),
                    fields)), primarykey)
        attrs['__delete__'] = 'DELETE FROM `%s` WHERE `%s` = ?' % (tableName,
                                                                   primarykey)
        return type.__new__(cls, name, bases, attrs)


class Field(object):
    '定义数据列基类'

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __repr__(self):
        return '<Field %s>: %s' % (self.column_type, self.name)


class StringField(Field):
    '定义字符串数据列'

    def __init__(self,
                 name=None,
                 primary_key=False,
                 default=None,
                 column_type='varchar(100)'):
        super().__init__(name, column_type, primary_key, default)


class BooleanField(Field):
    '定义布尔值数据列'

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    '定义整数数据列'

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    '定义浮点数数据列'

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    '定义文本数据列'

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)
