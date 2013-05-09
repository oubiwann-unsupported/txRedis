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


class PubSubCommandsTestCase(CommandsBaseTestCase):

    @defer.inlineCallbacks
    def setUp(self):
        yield CommandsBaseTestCase.setUp(self)

        class TestSubscriber(RedisSubscriber):

            def __init__(self, *args, **kwargs):
                RedisSubscriber.__init__(self, *args, **kwargs)
                self.msg_channel = None
                self.msg_message = None
                self.msg_received = defer.Deferred()
                self.channel_subscribed = defer.Deferred()

            def messageReceived(self, channel, message):
                self.msg_channel = channel
                self.msg_message = message
                self.msg_received.callback(None)
                self.msg_received = defer.Deferred()

            def channelSubscribed(self, channel, numSubscriptions):
                self.channel_subscribed.callback(None)
                self.channel_subscribed = defer.Deferred()
            channelUnsubscribed = channelSubscribed
            channelPatternSubscribed = channelSubscribed
            channelPatternUnsubscribed = channelSubscribed

        clientCreator = protocol.ClientCreator(reactor, TestSubscriber)
        self.subscriber = yield clientCreator.connectTCP(REDIS_HOST,
                                                         REDIS_PORT)

    def tearDown(self):
        CommandsBaseTestCase.tearDown(self)
        self.subscriber.transport.loseConnection()

    @defer.inlineCallbacks
    def test_subscribe(self):
        s = self.subscriber
        t = self.assertEqual

        cb = s.channel_subscribed
        yield s.subscribe("channelA")
        yield cb

        cb = s.msg_received
        a = yield self.redis.publish("channelA", "dataB")
        ex = 1
        t(a, ex)
        yield cb
        a = s.msg_channel
        ex = "channelA"
        t(a, ex)
        a = s.msg_message
        ex = "dataB"
        t(a, ex)

    @defer.inlineCallbacks
    def test_unsubscribe(self):
        s = self.subscriber

        cb = s.channel_subscribed
        yield s.subscribe("channelA", "channelB", "channelC")
        yield cb

        cb = s.channel_subscribed
        yield s.unsubscribe("channelA", "channelC")
        yield cb

        yield s.unsubscribe()

    @defer.inlineCallbacks
    def test_psubscribe(self):
        s = self.subscriber
        t = self.assertEqual

        cb = s.channel_subscribed
        yield s.psubscribe("channel*", "magic*")
        yield cb

        cb = s.msg_received
        a = yield self.redis.publish("channelX", "dataC")
        ex = 1
        t(a, ex)
        yield cb
        a = s.msg_channel
        ex = "channelX"
        t(a, ex)
        a = s.msg_message
        ex = "dataC"
        t(a, ex)

    @defer.inlineCallbacks
    def test_punsubscribe(self):
        s = self.subscriber

        cb = s.channel_subscribed
        yield s.psubscribe("channel*", "magic*", "woot*")
        yield cb

        cb = s.channel_subscribed
        yield s.punsubscribe("channel*", "woot*")
        yield cb
        yield s.punsubscribe()