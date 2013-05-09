from datetime import datetime
import sys

from twisted.internet import defer, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log

from txredis.client import (RedisClientFactory, RedisSubscriber,
    RedisSubscriberFactory)


class Subscriber(RedisSubscriber):

    numChannels = 0

    def channelSubscribed(self, channel, numSubscribed):
        self.numChannels += 1

    def channelUnsubscribed(self, channel, numSubscribed):
        self.numChannels -= 1

    def messageReceived(self, channel, message):
        log.msg("got message %r on channel %s" % (message, channel))


class SubscriberFactory(RedisSubscriberFactory):

    protocol = Subscriber


def processSubscription(result, publisher, subscriber):
    log.msg("Status of subscription request: %s" % result)
    return (publisher, subscriber)


def subscribeToChannel(publisher, subscriber):
    log.msg("Subscribing to channel ...")
    import pdb;pdb.set_trace()
    d = subscriber.subscribe("awesome-channel")
    d.addErrback(log.err)
    d.addCallback(processSubscription, publisher, subscriber)
    return d


def setUpPublisher(subscriber):
    log.msg("Setting up publisher ...")
    publisherEndpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 6379)
    d = publisherEndpoint.connect(RedisClientFactory())
    d.addErrback(log.err)
    d.addCallback(subscribeToChannel, subscriber)
    return d


def setUpSubscriber():
    log.msg("Setting up subscriber ...")
    subscriberEndpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 6379)
    d = subscriberEndpoint.connect(SubscriberFactory())
    d.addErrback(log.err)
    d.addCallback(setUpPublisher)
    return d


def finish(ignore):
    reactor.stop()


def example():
    d = setUpSubscriber()
    d.addCallback(finish)
    return d


def runTest():
    redis1 = yield getRedisSubscriber()
    redis2 = yield getRedis()

    log.msg("redis1: SUBSCRIBE w00t")
    response = yield redis1.subscribe("w00t")
    log.msg("subscribed to w00t, response = %r" % response)

    while redis1.numChannels == 0:
        d = defer.Deferred()
        reactor.callLater(0.1, d.callback, True)
        yield d

    log.msg("redis2: PUBLISH w00t 'Hello, world!'")
    response = yield redis2.publish("w00t", "Hello, world!")
    log.msg("published to w00t, response = %r" % response)


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    example()
    reactor.run()