import logging
logging.basicConfig(level=logging.INFO)
import functools
import asyncio
import inspect
from aiohttp import web
from urllib import parse



def get(path):
    '''定义@get('/path')'''

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)

        wrapper.__method__ = 'GET'
        wrapper.__route__ = path
        return wrapper

    return decorator


def post(path):
    '''定义@post('/path')'''

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)

        wrapper.__method__ = 'POST'
        wrapper.__route__ = path
        return wrapper

    return decorator


#运用inspect模块，创建几个函数用于获取URL处理函数与request参数之间的关系
def get_required_kw_args(fn):
    '搜集没有默认值的命名关键字参数'
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param.default == inspect.Parameter.empty:
            args.append(name)
    return tuple(args)


def get_named_kw_args(fn):
    '搜集命名关键字参数'
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            args.append(name)
    return tuple(args)


def has_named_kw_arg(fn):
    '判断是否有命名关键字参数'
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            return True
    return False


def has_var_kw_arg(fn):
    '判断是否有关键字参数'
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True
    return False


def has_request_arg(fn):
    '判断是否有request命名关键字参数,并且是否在最后'
    params = inspect.signature(fn).parameters
    sig = inspect.signature(fn)
    found = False
    for name, param in params.items():
        if name == 'request':
            found = True
            continue
        if found and (param.kind != inspect.Parameter.VAR_POSITIONAL
                      and param.kind != inspect.Parameter.KEYWORD_ONLY
                      and param.kind != inspect.Parameter.VAR_KEYWORD):
            raise ValueError(
                'request parameter must be the last named parameter in function: %s%s'
                % (fn.__name__, str(sig)))
    return found


class RequestHandler(object):
    '定义URL处理函数类'

    def __init__(self, app, fn):
        self._app = app
        self._fn = fn
        self._has_request_arg = has_request_arg(fn)
        self._has_var_kw_arg = has_var_kw_arg(fn)
        self._has_named_kw_arg = has_named_kw_arg(fn)
        self._named_kw_args = get_named_kw_args(fn)
        self._required_kw_args = get_required_kw_args(fn)

    async def __call__(self, request):
        kw = None
        if self._has_named_kw_arg or self._has_var_kw_arg:
            if request.method == 'POST':
                if not request.content_type:
                    return web.HTTPBadRequest(text='Missing Content Type.')
                ct = request.content_type.lower()
                if ct.startswith('application/json'):
                    params = await request.json()
                    if not isinstance(params, dict):
                        return web.HTTPBadRequest(
                            text='JSON body must be object.')
                    kw = params
                elif ct.startswith('application/x-www-form-urlencode'
                                   ) or ct.startswith('multipart/form-data'):
                    params = await request.post()
                    kw = dict(**params)
                else:
                    return web.HTTPBadRequest(
                        text='Unsupported Content Type: %s' % request.
                        content_type)
            if request.method == 'GET':
                qs = request.query_string
                if qs:
                    kw = dict()
                    for k, v in parse.parse_qs(qs, True).items():
                        kw[k] = v[0]
        if kw is None:
            kw = dict(**request.match_info)
        else:
            if not self._has_var_kw_arg and self._named_kw_args:
                # remove all unamed kw:
                copy = dict()
                for name in self._named_kw_args:
                    if name in kw:
                        copy[name] = kw[name]
                kw = copy
            # check named arg:
            for k, v in request.match_info.items():
                if k in kw:
                    logging.warning(
                        'Duplicate arg name in named arg and kw args: %s' % k)
                kw[k] = v
        if self._has_request_arg:
            kw['request'] = request
        # check required kw:
        if self._required_kw_args:
            for name in self._required_kw_args:
                if not name in kw:
                    return web.HTTPBadRequest(
                        text='Missing argument: %s' % name)
        logging.info('call with args: %s' % str(kw))


'''        try:
            r = await self._fn(**kw)
            return r
        except APIError as e:
            return dict(error=e.error, data=e.data, message=e.message)'''
