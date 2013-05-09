from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import defer

from txredis.client import Redis
from txredis.exceptions import ResponseError
from txredis.testing import CommandsBaseTestCase, REDIS_HOST, REDIS_PORT


class ListsCommandsTestCase(CommandsBaseTestCase):
    """Test commands that operate on lists.
    """

    @defer.inlineCallbacks
    def test_blank_item(self):
        key = 'test:list'
        yield self.redis.delete(key)

        chars = ["a", "", "c"]
        for char in chars:
            yield self.redis.push(key, char)

        r = yield self.redis.lrange(key, 0, len(chars))
        self.assertEquals(["c", "", "a"], r)

    @defer.inlineCallbacks
    def test_concurrent(self):
        """Test ability to handle many large responses at the same time"""
        num_lists = 100
        items_per_list = 50

        # 1. Generate and fill lists
        lists = []
        for l in range(0, num_lists):
            key = 'list-%d' % l
            yield self.redis.delete(key)
            for i in range(0, items_per_list):
                yield self.redis.push(key, 'item-%d' % i)
            lists.append(key)

        # 2. Make requests to get all lists
        ds = []
        for key in lists:
            d = self.redis.lrange(key, 0, items_per_list)
            ds.append(d)

        # 3. Wait on all responses and make sure we got them all
        r = yield defer.DeferredList(ds)
        self.assertEquals(len(r), num_lists)

    @defer.inlineCallbacks
    def test_push(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.push('l', 'a')
        ex = 1
        t(a, ex)
        a = yield r.set('a', 'a')
        ex = 'OK'
        t(a, ex)

        yield r.delete('l')
        a = yield r.push('l', 'a', no_create=True)
        ex = 0
        t(a, ex)

        a = yield r.push('l', 'a', tail=True, no_create=True)
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_push_variable(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        yield r.lpush('l', 'a', 'b', 'c', 'd')
        a = yield r.llen('l')
        ex = 4
        t(a, ex)

        yield r.rpush('l', 't', 'u', 'v', 'w')
        a = yield r.llen('l')
        ex = 8
        t(a, ex)

    @defer.inlineCallbacks
    def test_llen(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.push('l', 'a')
        ex = 1
        t(a, ex)
        a = yield r.llen('l')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'a')
        ex = 2
        t(a, ex)
        a = yield r.llen('l')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_lrange(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.lrange('l', 0, 1)
        ex = []
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'aaa']
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.lrange('l', 0, 0)
        ex = [u'bbb']
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'bbb', u'aaa']
        t(a, ex)
        a = yield r.lrange('l', -1, 0)
        ex = []
        t(a, ex)
        a = yield r.lrange('l', -1, -1)
        ex = [u'aaa']
        t(a, ex)

    @defer.inlineCallbacks
    def test_ltrim(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('l')
        a = yield r.ltrim('l', 0, 1)
        ex = ResponseError('OK')
        t(str(a), str(ex))
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'ccc')
        ex = 3
        t(a, ex)
        a = yield r.ltrim('l', 0, 1)
        ex = 'OK'
        t(a, ex)
        a = yield r.llen('l')
        ex = 2
        t(a, ex)
        a = yield r.ltrim('l', 99, 95)
        ex = 'OK'
        t(a, ex)
        a = yield r.llen('l')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_lindex(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        yield r.lindex('l', 0)
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.lindex('l', 0)
        ex = u'aaa'
        t(a, ex)
        yield r.lindex('l', 2)
        a = yield r.push('l', 'ccc')
        ex = 2
        t(a, ex)
        a = yield r.lindex('l', 1)
        ex = u'aaa'
        t(a, ex)
        a = yield r.lindex('l', -1)
        ex = u'aaa'
        t(a, ex)

    @defer.inlineCallbacks
    def test_pop(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        yield r.pop('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.pop('l')
        ex = u'bbb'
        t(a, ex)
        a = yield r.pop('l')
        ex = u'aaa'
        t(a, ex)
        yield r.pop('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.pop('l', tail=True)
        ex = u'aaa'
        t(a, ex)
        a = yield r.pop('l')
        ex = u'bbb'
        t(a, ex)
        a = yield r.pop('l')
        ex = None
        t(a, ex)

    def test_lset_on_nonexistant_key(self):
        r = self.redis
        t = self.assertEqual

        d = r.delete('l')
        def bad_lset(_res):
            d = r.lset('l', 0, 'a')
            self.failUnlessFailure(d, ResponseError)
            def match_err(a):
                ex = ResponseError('ERR no such key')
                t(str(a), str(ex))
            d.addCallback(match_err)
            return d
        d.addCallback(bad_lset)
        return d

    def test_lset_bad_range(self):
        r = self.redis
        t = self.assertEqual

        d = r.delete('l')
        def proceed(_res):
            d = r.push('l', 'aaa')
            def done_push(a):
                ex = 1
                t(a, ex)
                d = r.lset('l', 1, 'a')
                self.failUnlessFailure(d, ResponseError)
                def check(a):
                    ex = ResponseError('ERR index out of range')
                    t(str(a), str(ex))
                d.addCallback(check)
                return d
            d.addCallback(done_push)
            return d
        d.addCallback(proceed)
        return d

    @defer.inlineCallbacks
    def test_lset(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.lset('l', 0, 'bbb')
        ex = 'OK'
        t(a, ex)
        a = yield r.lrange('l', 0, 1)
        ex = [u'bbb']
        t(a, ex)

    @defer.inlineCallbacks
    def test_lrem(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('l')
        a = yield r.push('l', 'aaa')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 3
        t(a, ex)
        a = yield r.lrem('l', 'aaa')
        ex = 2
        t(a, ex)
        a = yield r.lrange('l', 0, 10)
        ex = [u'bbb']
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 3
        t(a, ex)
        a = yield r.lrem('l', 'aaa', 1)
        ex = 1
        t(a, ex)
        a = yield r.lrem('l', 'aaa', 1)
        ex = 1
        t(a, ex)
        a = yield r.lrem('l', 'aaa', 1)
        ex = 0
        t(a, ex)


class BlockingListOperartionsTestCase(CommandsBaseTestCase):
    """@todo test timeout
    @todo robustly test async/blocking redis commands
    """
    @defer.inlineCallbacks
    def test_bpop_noblock(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('test.list.a')
        yield r.delete('test.list.b')
        yield r.push('test.list.a', 'stuff')
        yield r.push('test.list.a', 'things')
        yield r.push('test.list.b', 'spam')
        yield r.push('test.list.b', 'bee')
        yield r.push('test.list.b', 'honey')

        a = yield r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.a', 'things']
        t(a, ex)
        a = yield r.bpop(['test.list.b', 'test.list.a'])
        ex = ['test.list.b', 'honey']
        t(a, ex)
        a = yield r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.a', 'stuff']
        t(a, ex)
        a = yield r.bpop(['test.list.b', 'test.list.a'])
        ex = ['test.list.b', 'bee']
        t(a, ex)
        a = yield r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.b', 'spam']
        t(a, ex)

    @defer.inlineCallbacks
    def test_bpop_block(self):
        r = self.redis
        t = self.assertEqual

        clientCreator = protocol.ClientCreator(reactor, Redis)
        r2 = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)

        def _cb(reply, ex):
            t(reply, ex)

        yield r.delete('test.list.a')
        yield r.delete('test.list.b')

        d = r.bpop(['test.list.a', 'test.list.b'])
        ex = ['test.list.a', 'stuff']
        d.addCallback(_cb, ex)

        yield r2.push('test.list.a', 'stuff')

        yield d
        r2.transport.loseConnection()