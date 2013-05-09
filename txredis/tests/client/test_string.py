from twisted.internet import reactor
from twisted.internet import defer
from twisted.trial import unittest

from txredis.client import RedisClientFactory
from txredis.testing import CommandsBaseTestCase, REDIS_HOST, REDIS_PORT


class StringsCommandTestCase(CommandsBaseTestCase):
    """Test commands that operate on string values.
    """

    @defer.inlineCallbacks
    def test_blank(self):
        yield self.redis.set('a', "")

        r = yield self.redis.get('a')
        self.assertEquals("", r)

    @defer.inlineCallbacks
    def test_set(self):
        a = yield self.redis.set('a', 'pippo')
        self.assertEqual(a, 'OK')

        unicode_str = u'pippo \u3235'
        a = yield self.redis.set('a', unicode_str)
        self.assertEqual(a, 'OK')

        a = yield self.redis.get('a')
        self.assertEqual(a, unicode_str.encode('utf8'))

        a = yield self.redis.set('b', 105.2)
        self.assertEqual(a, 'OK')

        a = yield self.redis.set('b', 'xxx', preserve=True)
        self.assertEqual(a, 0)

        a = yield self.redis.setnx('b', 'xxx')
        self.assertEqual(a, 0)

        a = yield self.redis.get('b')
        self.assertEqual(a, '105.2')

    @defer.inlineCallbacks
    def test_get(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'pippo')
        t(a, 'OK')
        a = yield r.set('b', 15)
        t(a, 'OK')
        a = yield r.set('c', ' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n ')
        t(a, 'OK')
        a = yield r.set('d', '\\r\\n')
        t(a, 'OK')

        a = yield r.get('a')
        t(a, u'pippo')

        a = yield r.get('b')
        ex = '15'
        t(a, ex)

        a = yield r.get('d')
        ex = u'\\r\\n'
        t(a, ex)

        a = yield r.get('b')
        ex = '15'
        t(a, ex)

        a = yield r.get('c')
        ex = u' \\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n '
        t(a, ex)

        a = yield r.get('ajhsd')
        ex = None
        t(a, ex)

    @defer.inlineCallbacks
    def test_getset(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'pippo')
        ex = 'OK'
        t(a, ex)

        a = yield r.getset('a', 2)
        ex = u'pippo'
        t(a, ex)

    @defer.inlineCallbacks
    def test_mget(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'pippo')
        ex = 'OK'
        t(a, ex)
        a = yield r.set('b', 15)
        ex = 'OK'
        t(a, ex)
        a = yield r.set('c', '\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n')
        ex = 'OK'
        t(a, ex)
        a = yield r.set('d', '\\r\\n')
        ex = 'OK'
        t(a, ex)
        a = yield r.mget('a', 'b', 'c', 'd')
        ex = [u'pippo', '15',
              u'\\r\\naaa\\nbbb\\r\\ncccc\\nddd\\r\\n', u'\\r\\n']
        t(a, ex)

    @defer.inlineCallbacks
    def test_incr(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('a')
        ex = 1
        t(a, ex)
        a = yield r.incr('a')
        ex = 1
        t(a, ex)
        a = yield r.incr('a')
        ex = 2
        t(a, ex)
        a = yield r.incr('a', 2)
        ex = 4
        t(a, ex)

    @defer.inlineCallbacks
    def test_decr(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.get('a')
        if a:
            yield r.delete('a')

        a = yield r.decr('a')
        ex = -1
        t(a, ex)
        a = yield r.decr('a')
        ex = -2
        t(a, ex)
        a = yield r.decr('a', 5)
        ex = -7
        t(a, ex)

    @defer.inlineCallbacks
    def test_setbit(self):
        r = self.redis
        yield r.delete('bittest')

        # original value is 0 when value is empty
        orig = yield r.setbit('bittest', 0, 1)
        self.assertEqual(orig, 0)

        # original value is 1 from above
        orig = yield r.setbit('bittest', 0, 0)
        self.assertEqual(orig, 1)

    @defer.inlineCallbacks
    def test_getbit(self):
        r = self.redis
        yield r.delete('bittest')

        yield r.setbit('bittest', 10, 1)
        a = yield r.getbit('bittest', 10)
        self.assertEqual(a, 1)

    @defer.inlineCallbacks
    def test_bitcount(self):
        r = self.redis
        # TODO tearDown or setUp should flushdb?
        yield r.delete('bittest')

        yield r.setbit('bittest', 10, 1)
        yield r.setbit('bittest', 25, 1)
        yield r.setbit('bittest', 3, 1)
        ct = yield r.bitcount('bittest')
        self.assertEqual(ct, 3)

    @defer.inlineCallbacks
    def test_bitcount_with_start_and_end(self):
        r = self.redis
        yield r.delete('bittest')

        yield r.setbit('bittest', 10, 1)
        yield r.setbit('bittest', 25, 1)
        yield r.setbit('bittest', 3, 1)
        ct = yield r.bitcount('bittest', 1, 2)
        self.assertEqual(ct, 1)


class TestFactory(CommandsBaseTestCase):

    def setUp(self):
        d = CommandsBaseTestCase.setUp(self)
        def do_setup(_res):
            self.factory = RedisClientFactory()
            reactor.connectTCP(REDIS_HOST, REDIS_PORT, self.factory)
            d = self.factory.deferred
            def cannot_connect(_res):
                raise unittest.SkipTest('Cannot connect to Redis.')
            d.addErrback(cannot_connect)
            return d
        d.addCallback(do_setup)
        return d

    def tearDown(self):
        CommandsBaseTestCase.tearDown(self)
        self.factory.continueTrying = 0
        self.factory.stopTrying()
        if self.factory.client:
            self.factory.client.setTimeout(None)
            self.factory.client.transport.loseConnection()

    @defer.inlineCallbacks
    def test_reconnect(self):
        a = yield self.factory.client.info()
        self.assertTrue('uptime_in_days' in a)

        # teardown the connection
        self.factory.client.transport.loseConnection()

        # wait until reconnected
        a = yield self.factory.deferred

        a = yield self.factory.client.info()
        self.assertTrue('uptime_in_days' in a)
    timeout = 4