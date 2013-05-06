from datetime import datetime
import sys

from twisted.internet import defer, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log

from txredis.client import RedisClientFactory


def doInsert(client):
    """
    This is just demonstrating client functionality and how you can use deferred
    lists to do a bunch of subsequent calls. A better thing here would be to use
    client.mset for setting mulitple key/value pairs at the same time.
    """
    log.msg("Preparing to insert a bunch of data ...")
    deferreds = []
    for x in xrange(10000):
        key = "key-%s" % x
        value = datetime.now().isoformat()
        d = client.set(key, value)
        d.addErrback(log.err)
        deferreds.append(d)
    return defer.DeferredList(deferreds)


def getResult(results):
    log.msg("Getting result ...")
    errors = 0
    successes = 0
    for deferredResult, serverResult in results:
        if deferredResult and serverResult == "OK":
            successes += 1
        else:
            errors += 1
    log.msg("Data insertion results: %s success and %s errors" % (
        successes, errors))


def finish(ignore):
    reactor.stop()


def example():
    endpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 6379)
    d = endpoint.connect(RedisClientFactory())
    d.addErrback(log.err)
    d.addCallback(doInsert)
    d.addCallback(getResult)
    d.addErrback(log.err)
    d.addCallback(finish)
    return d


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    example()
    reactor.run()