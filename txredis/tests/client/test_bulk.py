from twisted.internet import protocol
from twisted.internet import reactor
from twisted.internet import defer

from txredis.testing import CommandsBaseTestCase, REDIS_HOST, REDIS_PORT


class LargeMultiBulkTestCase(CommandsBaseTestCase):
    @defer.inlineCallbacks
    def test_large_multibulk(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        data = set(xrange(1, 100000))
        for i in data:
            r.sadd('s', i)
        res = yield r.smembers('s')
        t(res, set(map(str, data)))


class MultiBulkTestCase(CommandsBaseTestCase):
    @defer.inlineCallbacks
    def test_nested_multibulk(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('str1', 'str2', 'list1', 'list2')
        yield r.set('str1', 'str1')
        yield r.set('str2', 'str2')
        yield r.lpush('list1', 'b1')
        yield r.lpush('list1', 'a1')
        yield r.lpush('list2', 'b2')
        yield r.lpush('list2', 'a2')

        r.multi()
        r.get('str1')
        r.lrange('list1', 0, -1)
        r.get('str2')
        r.lrange('list2', 0, -1)
        r.get('notthere')

        a = yield r.execute()
        ex = ['str1', ['a1', 'b1'], 'str2', ['a2', 'b2'], None]
        t(a, ex)

        a = yield r.get('str2')
        ex = 'str2'
        t(a, ex)

    @defer.inlineCallbacks
    def test_empty_multibulk(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('list1')
        a = yield r.lrange('list1', 0, -1)
        ex = []
        t(a, ex)

    @defer.inlineCallbacks
    def test_null_multibulk(self):
        r = self.redis
        t = self.assertEqual

        clientCreator = protocol.ClientCreator(reactor, self.protocol)
        r2 = yield clientCreator.connectTCP(REDIS_HOST, REDIS_PORT)

        yield r.delete('a')

        r.watch('a')
        r.multi()
        yield r.set('a', 'a')
        yield r2.set('a', 'b')

        r2.transport.loseConnection()

        a = yield r.execute()
        ex = None
        t(a, ex)

