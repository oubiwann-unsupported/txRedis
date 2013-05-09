import time

from twisted.internet import error
from twisted.internet import defer
from twisted.internet.task import Clock
from twisted.test.proto_helpers import StringTransportWithDisconnection
from twisted.trial import unittest

from txredis.client import Redis
from txredis.exceptions import ResponseError
from txredis.testing import CommandsBaseTestCase


class GeneralCommandTestCase(CommandsBaseTestCase):
    """Test commands that operate on any type of redis value.
    """
    @defer.inlineCallbacks
    def test_ping(self):
        a = yield self.redis.ping()
        self.assertEqual(a, 'PONG')

    @defer.inlineCallbacks
    def test_config(self):
        t = self.assertEqual
        a = yield self.redis.get_config('*')
        self.assertTrue(isinstance(a, dict))
        self.assertTrue('dbfilename' in a)

        a = yield self.redis.set_config('dbfilename', 'dump.rdb.tmp')
        ex = 'OK'
        t(a, ex)

        a = yield self.redis.get_config('dbfilename')
        self.assertTrue(isinstance(a, dict))
        t(a['dbfilename'], 'dump.rdb.tmp')

    """
    @defer.inlineCallbacks
    def test_auth(self):
        r = self.redis
        t = self.assertEqual

        # set a password
        password = 'foobar'
        a = yield self.redis.set_config('requirepass', password)
        ex = 'OK'
        t(a, ex)

        # auth with it
        a = yield self.redis.auth(password)
        ex = 'OK'
        t(a, ex)

        # turn password off
        a = yield self.redis.set_config('requirepass', '')
        ex = 'OK'
        t(a, ex)
    """

    @defer.inlineCallbacks
    def test_exists(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.exists('dsjhfksjdhfkdsjfh')
        ex = 0
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.exists('a')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_delete(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('dsjhfksjdhfkdsjfh')
        ex = 0
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.delete('a')
        ex = 1
        t(a, ex)
        a = yield r.exists('a')
        ex = 0
        t(a, ex)
        a = yield r.delete('a')
        ex = 0
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.set('b', 'b')
        ex = 'OK'
        t(a, ex)
        a = yield r.delete('a', 'b')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_get_object(self):
        r = self.redis
        t = self.assertEqual
        a = yield r.set('obj', 1)
        ex = 'OK'
        t(a, ex)

        a = yield r.get_object('obj', idletime=True)
        self.assertEqual(type(a), int)

        a = yield r.get_object('obj', encoding=True)
        ex = 'int'
        t(a, ex)

    @defer.inlineCallbacks
    def test_get_type(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 3)
        ex = 'OK'
        t(a, ex)
        a = yield r.get_type('a')
        ex = 'string'
        t(a, ex)
        a = yield r.get_type('zzz')
        ex = None
        t(a, ex)
        self.assertTrue(a == None or a == 'none')

    @defer.inlineCallbacks
    def test_keys(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.flush()
        ex = 'OK'
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.keys('a*')
        ex = [u'a']
        t(a, ex)
        a = yield r.set('a2', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.keys('a*')
        ex = [u'a', u'a2']
        t(a, ex)
        a = yield r.delete('a2')
        ex = 1
        t(a, ex)
        a = yield r.keys('sjdfhskjh*')
        ex = []
        t(a, ex)

    @defer.inlineCallbacks
    def test_randomkey(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield isinstance((yield r.randomkey()), str)
        ex = True
        t(a, ex)

    def test_rename_same_src_dest(self):
        r = self.redis
        t = self.assertEqual
        d = r.rename('a', 'a')
        self.failUnlessFailure(d, ResponseError)
        def test_err(a):
            ex = ResponseError('ERR source and destination objects are the same')
            t(str(a), str(ex))
        d.addCallback(test_err)
        return d

    @defer.inlineCallbacks
    def test_rename(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.rename('a', 'b')
        ex = 'OK'
        t(a, ex)
        a = yield r.get('a')
        t(a, None)
        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.rename('b', 'a', preserve=True)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_dbsize(self):
        r = self.redis
        t = self.assertTrue
        a = yield r.dbsize()
        t(isinstance(a, int) or isinstance(a, long))

    @defer.inlineCallbacks
    def test_expire(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.expire('a', 1)
        ex = 1
        t(a, ex)
        a = yield r.expire('zzzzz', 1)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_expireat(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.expireat('a', int(time.time() + 10))
        ex = 1
        t(a, ex)
        a = yield r.expireat('zzzzz', int(time.time() + 10))
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_setex(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.set('q', 1, expire=10)
        ex = 'OK'
        t(a, ex)
        # the following checks the expected response of an EXPIRE on a key with
        # an existing TTL. unfortunately the behaviour of redis changed in
        # v2.1.3 so we have to determine which behaviour to expect...
        info = yield r.info()
        redis_vern = tuple(map(int, info['redis_version'].split('.')))
        if redis_vern < (2, 1, 3):
            ex = 0
        else:
            ex = 1
        a = yield r.expire('q', 1)
        t(a, ex)

    @defer.inlineCallbacks
    def test_mset(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.mset({'ma': 1, 'mb': 2})
        ex = 'OK'
        t(a, ex)

        a = yield r.mset({'ma': 1, 'mb': 2}, preserve=True)
        ex = 0

        a = yield r.msetnx({'ma': 1, 'mb': 2})
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_substr(self):
        r = self.redis
        t = self.assertEqual

        string = 'This is a string'
        r.set('s', string)
        a = yield r.substr('s', 0, 3) # old name
        ex = 'This'
        t(a, ex)
        a = yield r.getrange('s', 0, 3) # new name
        ex = 'This'
        t(a, ex)

    @defer.inlineCallbacks
    def test_append(self):
        r = self.redis
        t = self.assertEqual

        string = 'some_string'
        a = yield r.set('q', string)
        ex = 'OK'
        t(a, ex)

        addition = 'foo'
        a = yield r.append('q', addition)
        ex = len(string + addition)
        t(a, ex)

    @defer.inlineCallbacks
    def test_ttl(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.ttl('a')
        ex = -1
        t(a, ex)
        a = yield r.expire('a', 10)
        ex = 1
        t(a, ex)
        a = yield r.ttl('a')
        ex = 10
        t(a, ex)
        a = yield r.expire('a', 0)
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_select(self):
        r = self.redis
        t = self.assertEqual

        yield r.select(9)
        yield r.delete('a')
        a = yield r.select(10)
        ex = 'OK'
        t(a, ex)
        a = yield r.set('a', 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.select(9)
        ex = 'OK'
        t(a, ex)
        a = yield r.get('a')
        ex = None
        t(a, ex)

    @defer.inlineCallbacks
    def test_move(self):
        r = self.redis
        t = self.assertEqual

        yield r.select(9)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)
        a = yield r.select(10)
        ex = 'OK'
        t(a, ex)
        if (yield r.get('a')):
            yield r.delete('a')
        a = yield r.select(9)
        ex = 'OK'
        t(a, ex)
        a = yield r.move('a', 10)
        ex = 1
        t(a, ex)
        yield r.get('a')
        a = yield r.select(10)
        ex = 'OK'
        t(a, ex)
        a = yield r.get('a')
        ex = u'a'
        t(a, ex)
        a = yield r.select(9)
        ex = 'OK'
        t(a, ex)

    @defer.inlineCallbacks
    def test_flush(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.flush()
        ex = 'OK'
        t(a, ex)

    def test_lastsave(self):
        r = self.redis
        t = self.assertEqual

        tme = int(time.time())
        d = r.save()
        def done_save(a):
            ex = 'OK'
            t(a, ex)
            d = r.lastsave()
            def got_lastsave(a):
                a = a >= tme
                ex = True
                t(a, ex)
            d.addCallback(got_lastsave)
            return d

        def save_err(res):
            if 'Background save already in progress' in str(res):
                return True
            else:
                raise res
        d.addCallbacks(done_save, save_err)
        return d

    @defer.inlineCallbacks
    def test_info(self):
        r = self.redis
        t = self.assertEqual

        info = yield r.info()
        a = info and isinstance(info, dict)
        ex = True
        t(a, ex)
        a = isinstance((yield info.get('connected_clients')), int)
        ex = True
        t(a, ex)

    @defer.inlineCallbacks
    def test_multi(self):
        r = yield self.redis.multi()
        self.assertEqual(r, 'OK')

    @defer.inlineCallbacks
    def test_execute(self):
        # multi with two sets
        yield self.redis.multi()
        r = yield self.redis.set('foo', 'bar')
        self.assertEqual(r, 'QUEUED')
        r = yield self.redis.set('foo', 'barbar')
        self.assertEqual(r, 'QUEUED')
        r = yield self.redis.execute()
        self.assertEqual(r, ['OK', 'OK'])
        r = yield self.redis.get('foo')
        self.assertEqual(r, 'barbar')

    def test_discard(self):
        d = self.redis.execute()
        # discard without multi will return ResponseError
        d = self.failUnlessFailure(d, ResponseError)

        # multi with two sets
        def step1(_res):
            d = self.redis.set('foo', 'bar1')

            def step2(_res):
                d = self.redis.multi()
                def in_multi(_res):
                    d = self.redis.set('foo', 'bar2')
                    def step3(_res):
                        d = self.redis.discard()
                        def step4(r):
                            self.assertEqual(r, 'OK')
                            d = self.redis.get('foo')
                            def got_it(res):
                                self.assertEqual(res, 'bar1')
                            d.addCallback(got_it)
                            return d
                        d.addCallback(step4)
                        return d
                    d.addCallback(step3)
                    return d
                d.addCallback(in_multi)
                return d
            d.addCallback(step2)
            return d

        d.addCallback(step1)
        return d

    @defer.inlineCallbacks
    def test_watch(self):
        r = yield self.redis.watch('foo')
        self.assertEqual(r, 'OK')

    @defer.inlineCallbacks
    def test_unwatch(self):
        yield self.redis.watch('foo')
        r = yield self.redis.unwatch()
        self.assertEqual(r, 'OK')


class NetworkTestCase(unittest.TestCase):

    def setUp(self):
        self.proto = Redis()
        self.clock = Clock()
        self.proto.callLater = self.clock.callLater
        self.transport = StringTransportWithDisconnection()
        self.transport.protocol = self.proto
        self.proto.makeConnection(self.transport)

    def test_request_while_disconnected(self):
        # fake disconnect
        self.proto._disconnected = True

        d = self.proto.get('foo')
        self.assertFailure(d, RuntimeError)

        def checkMessage(error):
            self.assertEquals(str(error), 'Not connected')

        return d.addCallback(checkMessage)

    def test_disconnect_during_request(self):
        d1 = self.proto.get("foo")
        d2 = self.proto.get("bar")
        self.assertEquals(len(self.proto._request_queue), 2)

        self.transport.loseConnection()
        done = defer.DeferredList([d1, d2], consumeErrors=True)

        def checkFailures(results):
            self.assertEquals(len(self.proto._request_queue), 0)
            for success, result in results:
                self.assertFalse(success)
                result.trap(error.ConnectionDone)

        return done.addCallback(checkFailures)