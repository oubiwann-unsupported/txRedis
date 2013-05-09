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




class ScriptingCommandsTestCase(CommandsBaseTestCase):
    """
    Test for Lua scripting commands.
    """

    _skipped = False

    @defer.inlineCallbacks
    def setUp(self):
        yield CommandsBaseTestCase.setUp(self)
        if ScriptingCommandsTestCase._skipped:
            self.redis.transport.loseConnection()
            raise SkipTest(ScriptingCommandsTestCase._skipped)
        info = yield self.redis.info()
        if 'used_memory_lua' not in info:
            ScriptingCommandsTestCase._skipped = (
                    'Scripting commands not available in Redis version %s' %
                    info['redis_version'])
            self.redis.transport.loseConnection()
            raise SkipTest(ScriptingCommandsTestCase._skipped)

    @defer.inlineCallbacks
    def test_eval(self):
        r = self.redis
        t = self.assertEqual

        source = 'return "ok"'
        a = yield r.eval(source)
        ex = 'ok'
        t(a, ex)

        source = ('redis.call("SET", KEYS[1], ARGV[1]) '
                  'return redis.call("GET", KEYS[1])')
        a = yield r.eval(source, ('test_eval',), ('x',))
        ex = 'x'
        t(a, ex)

        source = 'return {ARGV[1], ARGV[2]}'
        a = yield r.eval(source, args=('a', 'b'))
        ex = ['a', 'b']
        t(a, ex)

    @defer.inlineCallbacks
    def test_evalsha(self):
        r = self.redis
        t = self.assertEqual

        source = 'return "ok"'
        yield r.eval(source)
        sha1 = hashlib.sha1(source).hexdigest()
        a = yield r.evalsha(sha1)
        ex = 'ok'
        t(a, ex)

        source = ('redis.call("SET", KEYS[1], ARGV[1]) '
                  'return redis.call("GET", KEYS[1])')
        yield r.eval(source, ('test_eval2',), ('x',))
        sha1 = hashlib.sha1(source).hexdigest()
        a = yield r.evalsha(sha1, ('test_eval3',), ('y',))
        ex = 'y'
        t(a, ex)

        source = 'return {ARGV[1], ARGV[2]}'
        yield r.eval(source, args=('a', 'b'))
        sha1 = hashlib.sha1(source).hexdigest()
        a = yield r.evalsha(sha1, args=('c', 'd'))
        ex = ['c', 'd']
        t(a, ex)

    def test_no_script(self):
        r = self.redis
        sha1 = hashlib.sha1('banana').hexdigest()
        d = r.evalsha(sha1)
        self.assertFailure(d, NoScript)
        return d

    @defer.inlineCallbacks
    def test_script_load(self):
        r = self.redis
        t = self.assertEqual

        source = ('redis.call("SET", KEYS[1], ARGV[1]) '
                  'return redis.call("GET", KEYS[1])')
        a = yield r.script_load(source)
        ex = hashlib.sha1(source).hexdigest()
        t(a, ex)

    @defer.inlineCallbacks
    def test_script_exists(self):
        r = self.redis
        t = self.assertEqual

        source = ('redis.call("SET", KEYS[1], ARGV[1]) '
                  'return redis.call("GET", KEYS[1])')
        yield r.script_load(source)
        script1 = hashlib.sha1(source).hexdigest()
        script2 = hashlib.sha1('banana').hexdigest()

        a = yield r.script_exists(script1, script2)
        ex = [True, False]
        t(a, ex)

    @defer.inlineCallbacks
    def test_script_flush(self):
        r = self.redis
        t = self.assertEqual

        source = ('redis.call("SET", KEYS[1], ARGV[1]) '
                  'return redis.call("GET", KEYS[1])')
        yield r.script_load(source)
        script1 = hashlib.sha1(source).hexdigest()
        source = 'return "ok"'
        yield r.script_load(source)
        script2 = hashlib.sha1(source).hexdigest()

        yield r.script_flush()
        a = yield r.script_exists(script1, script2)
        ex = [False, False]
        t(a, ex)

    def test_script_kill(self):
        r = self.redis
        t = self.assertEqual

        def eb(why):
            t(str(why.value), 'ERR No scripts in execution right now.')
            return why

        d = r.script_kill()
        d.addErrback(eb)
        self.assertFailure(d, ResponseError)

        return d