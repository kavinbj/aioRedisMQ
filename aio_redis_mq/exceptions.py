"""
name: exceptions
author：kavinbj
createdAt: 2021/7/14
version: 2.0.0
description:

"""
from aioredis import exceptions


class AioRedisMQException(exceptions.RedisError):
    pass


class AioGroupError(AioRedisMQException):
    pass


class AioGroupCreateError(AioRedisMQException):
    pass


class AioRedisMQValueError(AioRedisMQException):
    pass
