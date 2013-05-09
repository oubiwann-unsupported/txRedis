from twisted.internet import defer

from txredis.testing import CommandsBaseTestCase


class SetsCommandsTestCase(CommandsBaseTestCase):
    """Test commands that operate on sets.
    """

    @defer.inlineCallbacks
    def test_blank(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sadd('s', "")
        ex = 1
        t(a, ex)
        a = yield r.smembers('s')
        ex = set([""])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sadd(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s', 'b')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_sadd_variable(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sadd('s', 'a', 'b', 'c', 'd')
        ex = 4
        a = yield r.scard('s')
        ex = 4
        t(a, ex)

    @defer.inlineCallbacks
    def test_sdiff(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        yield r.delete('t')
        yield r.sadd('s', 'a')
        yield r.sadd('s', 'b')
        yield r.sadd('t', 'a')
        a = yield r.sdiff('s', 't')
        ex = ['b']
        t(a, ex)

        a = yield r.sdiffstore('c', 's', 't')
        ex = 1
        t(a, ex)

        a = yield r.scard('c')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_srandmember(self):
        r = self.redis

        yield r.delete('s')
        yield r.sadd('s', 'a')
        yield r.sadd('s', 'b')
        yield r.sadd('s', 'c')
        a = yield r.srandmember('s')
        self.assertTrue(a in set(['a', 'b', 'c']))

    @defer.inlineCallbacks
    def test_smove(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        yield r.delete('t')
        yield r.sadd('s', 'a')
        yield r.sadd('t', 'b')
        a = yield r.smove('s', 't', 'a')
        ex = 1
        t(a, ex)
        a = yield r.scard('s')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_srem(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.srem('s', 'aaa')
        ex = 0
        t(a, ex)
        a = yield r.sadd('s', 'b')
        ex = 1
        t(a, ex)
        a = yield r.srem('s', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sismember('s', 'b')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_srem_variable(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sadd('s', 'a', 'b', 'c', 'd')
        ex = 4
        t(a, ex)
        a = yield r.srem('s', 'a', 'b')
        ex = 2
        t(a, ex)
        a = yield r.scard('s')
        ex = 2
        t(a, ex)

    @defer.inlineCallbacks
    def test_spop(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('s')

        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)

        a = yield r.spop('s')
        ex = u'a'
        t(a, ex)

    @defer.inlineCallbacks
    def test_scard(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('s')

        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)

        a = yield r.scard('s')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_sismember(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s')
        a = yield r.sismember('s', 'b')
        ex = 0
        t(a, ex)
        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sismember('s', 'b')
        ex = 0
        t(a, ex)
        a = yield r.sismember('s', 'a')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_sinter(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sinter('s1', 's2', 's3')
        ex = set([])
        t(a, ex)
        a = yield r.sinter('s1', 's2')
        ex = set([u'a'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sinterstore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sinterstore('s_s', 's1', 's2', 's3')
        ex = 0
        t(a, ex)
        a = yield r.sinterstore('s_s', 's1', 's2')
        ex = 1
        t(a, ex)
        a = yield r.smembers('s_s')
        ex = set([u'a'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_smembers(self):
        r = self.redis
        t = self.assertEqual

        a = yield r.delete('s')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s', 'b')
        ex = 1
        t(a, ex)
        a = yield r.smembers('s')
        ex = set([u'a', u'b'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sunion(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sunion('s1', 's2', 's3')
        ex = set([u'a', u'b'])
        t(a, ex)
        a = yield r.sadd('s2', 'c')
        ex = 1
        t(a, ex)
        a = yield r.sunion('s1', 's2', 's3')
        ex = set([u'a', u'c', u'b'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sunionstore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('s1')
        yield r.delete('s2')
        yield r.delete('s3')
        a = yield r.sadd('s1', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s2', 'a')
        ex = 1
        t(a, ex)
        a = yield r.sadd('s3', 'b')
        ex = 1
        t(a, ex)
        a = yield r.sunionstore('s4', 's1', 's2', 's3')
        ex = 2
        t(a, ex)
        a = yield r.smembers('s4')
        ex = set([u'a', u'b'])
        t(a, ex)

    @defer.inlineCallbacks
    def test_sort_style(self):
        # considering, given that redis only stores strings, whether the sort it
        # provides is a numeric or a lexicographical sort; turns out that it's
        # numeric; i.e. redis is doing implicit type coercion for the sort of
        # numeric values.  This test serves to document that, and to a lesser
        # extent check for regression in the implicit str() marshalling of
        # txredis
        r = self.redis
        t = self.assertEqual
        yield r.delete('l')
        items = [ 007, 10, -5, 0.1, 100, -3, 20, 0.02, -3.141 ]
        for i in items:
            yield r.push('l', i, tail=True)
        a = yield r.sort('l')
        ex = map(str, sorted(items))
        t(a, ex)

    @defer.inlineCallbacks
    def test_sort(self):
        r = self.redis
        t = self.assertEqual
        s = lambda l: map(str, l)

        yield r.delete('l')
        a = yield r.push('l', 'ccc')
        ex = 1
        t(a, ex)
        a = yield r.push('l', 'aaa')
        ex = 2
        t(a, ex)
        a = yield r.push('l', 'ddd')
        ex = 3
        t(a, ex)
        a = yield r.push('l', 'bbb')
        ex = 4
        t(a, ex)
        a = yield r.sort('l', alpha=True)
        ex = [u'aaa', u'bbb', u'ccc', u'ddd']
        t(a, ex)
        a = yield r.delete('l')
        ex = 1
        t(a, ex)
        for i in range(1, 5):
            yield r.push('l', 1.0 / i, tail=True)
        a = yield r.sort('l')
        ex = s([0.25, 0.333333333333, 0.5, 1.0])
        t(a, ex)
        a = yield r.sort('l', desc=True)
        ex = s([1.0, 0.5, 0.333333333333, 0.25])
        t(a, ex)
        a = yield r.sort('l', desc=True, start=2, num=1)
        ex = s([0.333333333333])
        t(a, ex)
        a = yield r.set('weight_0.5', 10)
        ex = 'OK'
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*')
        ex = s([0.5, 1.0, 0.333333333333, 0.25])
        t(a, ex)
        for i in (yield r.sort('l', desc=True)):
            yield r.set('test_%s' % i, 100 - float(i))
        a = yield r.sort('l', desc=True, get='test_*')
        ex = s([99.0, 99.5, 99.6666666667, 99.75])
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*', get='test_*')
        ex = s([99.5, 99.0, 99.6666666667, 99.75])
        t(a, ex)
        a = yield r.sort('l', desc=True, by='weight_*', get='missing_*')
        ex = [None, None, None, None]
        t(a, ex)

    @defer.inlineCallbacks
    def test_large_values(self):
        import uuid
        import random
        r = self.redis
        t = self.assertEqual

        for i in range(5):
            key = str(uuid.uuid4())
            value = random.randrange(10**40000, 11**40000)
            a = yield r.set(key, value)
            t('OK', a)
            rval = yield r.get(key)
            t(rval, str(value))


class SortedSetCommandsTestCase(CommandsBaseTestCase):
    """Test commands that operate on sorted sets.
    """
    @defer.inlineCallbacks
    def test_basic(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        a = yield r.zadd('z', 'a', 1)
        ex = 1
        t(a, ex)
        yield r.zadd('z', 'b', 2.142)

        a = yield r.zrank('z', 'a')
        ex = 0
        t(a, ex)

        a = yield r.zrank('z', 'a', reverse=True)
        ex = 1
        t(a, ex)

        a = yield r.zcard('z')
        ex = 2
        t(a, ex)

        a = yield r.zscore('z', 'b')
        ex = 2.142
        t(a, ex)

        a = yield r.zrange('z', 0, -1, withscores=True)
        ex = [('a', 1), ('b', 2.142)]
        t(a, ex)

        a = yield r.zrem('z', 'a')
        ex = 1
        t(a, ex)

    @defer.inlineCallbacks
    def test_zcount(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1)
        yield r.zadd('z', 'b', 2)
        yield r.zadd('z', 'c', 3)
        yield r.zadd('z', 'd', 4)
        a = yield r.zcount('z', 1, 3)
        ex = 3
        t(a, ex)

    @defer.inlineCallbacks
    def test_zremrange(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1.0)
        yield r.zadd('z', 'b', 2.0)
        yield r.zadd('z', 'c', 3.0)
        yield r.zadd('z', 'd', 4.0)

        a = yield r.zremrangebyscore('z', 1.0, 3.0)
        ex = 3
        t(a, ex)

        yield r.zadd('z', 'a', 1.0)
        yield r.zadd('z', 'b', 2.0)
        yield r.zadd('z', 'c', 3.0)
        a = yield r.zremrangebyrank('z', 0, 2)
        ex = 3
        t(a, ex)

    @defer.inlineCallbacks
    def test_add_variable(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1.0)
        a = yield r.zcard('z')
        ex = 1
        t(a, ex)

        # NB. note how for multiple argument it's score then val
        yield r.zadd('z', 2.0, 'b', 3.0, 'c')
        a = yield r.zcard('z')
        ex = 3

    @defer.inlineCallbacks
    def test_zrem_variable(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        yield r.zadd('z', 'a', 1.0)
        a = yield r.zcard('z')
        ex = 1
        t(a, ex)

        # NB. note how for multiple argument it's score then val
        yield r.zadd('z', 2.0, 'b', 3.0, 'c')
        a = yield r.zcard('z')
        ex = 3
        t(a, ex)

        yield r.zrem('z', 'a', 'b', 'c')
        a = yield r.zcard('z')
        ex = 0
        t(a, ex)

    @defer.inlineCallbacks
    def test_zrangebyscore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('z')
        a = yield r.zrangebyscore('z', -1, -1, withscores=True)
        ex = []
        t(a, ex)

        yield r.zadd('z', 'a', 1.014)
        yield r.zadd('z', 'b', 4.252)
        yield r.zadd('z', 'c', 0.232)
        yield r.zadd('z', 'd', 10.425)
        a = yield r.zrangebyscore('z')
        ex = ['c', 'a', 'b', 'd']
        t(a, ex)

        a = yield r.zrangebyscore('z', offset=1, count=2)
        ex = ['a', 'b']
        t(a, ex)

        a = yield r.zrangebyscore('z', offset=1, count=2, withscores=True)
        ex = [('a', 1.014), ('b', 4.252)]
        t(a, ex)

        a = yield r.zrangebyscore(
            'z', min=1, offset=1, count=2, withscores=True)
        ex = [('b', 4.252), ('d', 10.425)]

    @defer.inlineCallbacks
    def test_zscore_and_zrange_nonexistant(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('a')
        a = yield r.zscore('a', 'somekey')
        t(a, None)

        yield r.delete('a')
        a = yield r.zrange('a', 0, -1, withscores=True)
        t(a, [])

    @defer.inlineCallbacks
    def test_zaggregatestore(self):
        r = self.redis
        t = self.assertEqual

        yield r.delete('a')
        yield r.delete('b')
        yield r.delete('t')

        yield r.zadd('a', 'a', 1.0)
        yield r.zadd('a', 'b', 2.0)
        yield r.zadd('a', 'c', 3.0)
        yield r.zadd('b', 'a', 1.0)
        yield r.zadd('b', 'b', 2.0)
        yield r.zadd('b', 'c', 3.0)

        a = yield r.zunionstore('t', ['a', 'b'])
        ex = 3
        t(a, ex)

        a = yield r.zscore('t', 'a')
        ex = 2
        t(a, ex)

        yield r.delete('t')
        a = yield r.zunionstore('t', {'a' : 2.0, 'b' : 2.0})
        ex = 3
        t(a, ex)

        a = yield r.zscore('t', 'a')
        ex = 4
        t(a, ex)

        yield r.delete('t')
        a = yield r.zunionstore('t', {'a' : 2.0, 'b' : 2.0}, aggregate='MAX')
        ex = 3
        t(a, ex)

        a = yield r.zscore('t', 'a')
        ex = 2
        t(a, ex)

        yield r.delete('t')
        a = yield r.zinterstore('t', {'a' : 2.0, 'b' : 2.0}, aggregate='MAX')
        ex = 3
        t(a, ex)