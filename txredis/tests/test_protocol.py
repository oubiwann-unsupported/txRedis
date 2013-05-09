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


class ProtocolTestCase(unittest.TestCase):

    def setUp(self):
        self.proto = Redis()
        self.transport = StringTransportWithDisconnection()
        self.transport.protocol = self.proto
        self.proto.makeConnection(self.transport)

    def sendResponse(self, data):
        self.proto.dataReceived(data)

    def test_error_response(self):
        # pretending 'foo' is a set, so get is incorrect
        d = self.proto.get("foo")
        self.assertEquals(self.transport.value(), '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
        msg = "Operation against a key holding the wrong kind of value"
        self.sendResponse("-%s\r\n" % msg)
        self.failUnlessFailure(d, ResponseError)
        def check_err(r):
            self.assertEquals(str(r), msg)
        return d

    @defer.inlineCallbacks
    def test_singleline_response(self):
        d = self.proto.ping()
        self.assertEquals(self.transport.value(), '*1\r\n$4\r\nPING\r\n')
        self.sendResponse("+PONG\r\n")
        r = yield d
        self.assertEquals(r, 'PONG')

    @defer.inlineCallbacks
    def test_bulk_response(self):
        d = self.proto.get("foo")
        self.assertEquals(self.transport.value(), '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n')
        self.sendResponse("$3\r\nbar\r\n")
        r = yield d
        self.assertEquals(r, 'bar')

    @defer.inlineCallbacks
    def test_multibulk_response(self):
        d = self.proto.lrange("foo", 0, 1)
        expected = '*4\r\n$6\r\nLRANGE\r\n$3\r\nfoo\r\n$1\r\n0\r\n$1\r\n1\r\n'
        self.assertEquals(self.transport.value(), expected)
        self.sendResponse("*2\r\n$3\r\nbar\r\n$6\r\nlolwut\r\n")
        r = yield d
        self.assertEquals(r, ['bar', 'lolwut'])

    @defer.inlineCallbacks
    def test_integer_response(self):
        d = self.proto.dbsize()
        self.assertEquals(self.transport.value(), '*1\r\n$6\r\nDBSIZE\r\n')
        self.sendResponse(":1234\r\n")
        r = yield d
        self.assertEquals(r, 1234)


class ProtocolBufferingTestCase(ProtocolTestCase):

    def sendResponse(self, data):
        """Send a response one character at a time to test buffering"""
        for char in data:
            self.proto.dataReceived(char)