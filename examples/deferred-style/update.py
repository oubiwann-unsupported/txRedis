from datetime import datetime
import sys

from twisted.internet import defer, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log

from txredis.client import RedisClientFactory


def doUpdate(result, client):
    log.msg("Preparing to update the old key/valye pair ...")
    d = client.set("name", "jane doe")
    d.addErrback(log.err)
    return d


def doInsert(client):
    log.msg("Preparing to insert a key/value pair ...")
    d = client.set("name", "john doe")
    d.addErrback(log.err)
    d.addCallback(doUpdate, client)
    return d


def finish(ignore):
    reactor.stop()


def example():
    endpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 6379)
    d = endpoint.connect(RedisClientFactory())
    d.addErrback(log.err)
    d.addCallback(doInsert)
    d.addCallback(finish)
    return d


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    example()
    reactor.run()