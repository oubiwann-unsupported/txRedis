import time
import hashlib

from twisted.internet import error
from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import defer
from twisted.internet.task import Clock
from twisted.test.proto_helpers import StringTransportWithDisconnection
from twisted.trial import unittest
from twisted.trial.unittest import SkipTest

from txredis.client import Redis, RedisSubscriber, RedisClientFactory
from txredis.exceptions import ResponseError, NoScript
from txredis.testing import CommandsBaseTestCase, REDIS_HOST, REDIS_PORT


class HashCommandsTestCase(CommandsBaseTestCase):
    """Test commands that operate on hashes.
    """

    @defer.inlineCallbacks
    def test_blank(self):
        yield self.redis.delete('h')
        yield self.redis.hset('h', 'blank', "")
        a = yield self.redis.hget('h', 'blank')
        self.assertEquals(a, '')
        a = yield self.redis.hgetall('h')
        self.assertEquals(a, {'blank': ''})

    @defer.inlineCallbacks
    def test_cas(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('h')
        ex = 1
        t(a, ex)

        a = yield r.hsetnx('h', 'f', 'v')
        ex = 1
        t(a, ex)

        a = yield r.hsetnx('h', 'f', 'v')
        ex = 0
        t(a, ex)


    @defer.inlineCallbacks
    def test_basic(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')

        a = yield r.hexists('d', 'k')
        ex = 0
        t(a, ex)

        yield r.hset('d', 'k', 'v')

        a = yield r.hexists('d', 'k')
        ex = 1
        t(a, ex)

        a = yield r.hget('d', 'k')
        ex = {'k': 'v'}
        t(a, ex)
        a = yield r.hset('d', 'new', 'b', preserve=True)
        ex = 1
        t(a, ex)
        a = yield r.hset('d', 'new', 'b', preserve=True)
        ex = 0
        t(a, ex)
        yield r.hdelete('d', 'new')

        yield r.hset('d', 'f', 's')
        a = yield r.hgetall('d')
        ex = dict(k='v', f='s')
        t(a, ex)

        a = yield r.hgetall('foo')
        ex = {}
        t(a, ex)

        a = yield r.hget('d', 'notexist')
        ex = None
        t(a, ex)

        a = yield r.hlen('d')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_hdel_variable(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        yield r.hset('d', 'a', 'vala')
        yield r.hmset('d', {'a' : 'vala', 'b' : 'valb', 'c' : 'valc'})
        a = yield r.hdel('d', 'a', 'b', 'c')
        ex = 3
        t(a, ex)
        a = yield r.hgetall('d')
        ex = {}
        t(a, ex)

    @defer.inlineCallbacks
    def test_hincr(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        yield r.hset('d', 'k', 0)
        a = yield r.hincr('d', 'k')
        ex = 1
        t(a, ex)

        a = yield r.hincr('d', 'k')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_hget(self):
        r = self.redis
        t = self.assertEqual

        yield r.hdelete('key', 'field')
        yield r.hset('key', 'field', 'value1')
        a = yield r.hget('key', 'field')
        ex = {'field': 'value1'}
        t(a, ex)

    @defer.inlineCallbacks
    def test_hmget(self):
        r = self.redis
        t = self.assertEqual

        yield r.hdelete('d', 'k')
        yield r.hdelete('d', 'j')
        yield r.hset('d', 'k', 'v')
        yield r.hset('d', 'j', 'p')
        a = yield r.hget('d', ['k', 'j'])
        ex = {'k': 'v', 'j': 'p'}
        t(a, ex)

    @defer.inlineCallbacks
    def test_hmset(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        in_dict = dict(k='v', j='p')
        a = yield r.hmset('d', in_dict)
        ex = 'OK'
        t(a, ex)

        a = yield r.hgetall('d')
        ex = in_dict
        t(a, ex)

    @defer.inlineCallbacks
    def test_hkeys(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        in_dict = dict(k='v', j='p')
        yield r.hmset('d', in_dict)

        a = yield r.hkeys('d')
        ex = ['k', 'j']
        t(a, ex)

    @defer.inlineCallbacks
    def test_hvals(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('d')
        in_dict = dict(k='v', j='p')
        yield r.hmset('d', in_dict)

        a = yield r.hvals('d')
        ex = ['v', 'p']
        t(a, ex)