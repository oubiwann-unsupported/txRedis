"""
This example does two types of calls to get data about the stored data:
  1) first, it does a query getting all the keys that match a pattern
  2) then it does a multiple-get (sends a bunch of keys at once) to get actual
     values for those keys.

This is probably *not* the best way to do this, especially given that key
searches are expensive. However, it's certainly a good exercise in using
callbacks with txRedis ;-)

As better methods of querying are learned, more examples will be added that
demonstrate those.
"""
from datetime import datetime
import sys

from twisted.internet import defer, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log

from txredis.client import RedisClientFactory


def processResults(results):
    for value in results:
        log.msg("\t%s" % value)


def batchLookup(keys, client):
    d = client.mget(*keys)
    d.addErrback(log.err)
    d.addCallback(processResults)
    return d


def doContractorQuery(results, client):
    log.msg("Getting all contractors ...")
    d = client.keys("contractor:*")
    d.addErrback(log.msg)
    d.addCallback(batchLookup, client)
    return d


def doEmployeeQuery(results, client):
    log.msg("Getting all employees ...")
    d = client.keys("employee:*")
    d.addErrback(log.msg)
    d.addCallback(batchLookup, client)
    return d


def doEmployeeDevQuery(results, client):
    log.msg("Getting employees who are devs ...")
    d = client.keys("employee:dev:*")
    d.addErrback(log.msg)
    d.addCallback(batchLookup, client)
    return d


def doEmployeeLocalQuery(results, client):
    log.msg("Getting employees who are local ...")
    d = client.keys("employee:*:local")
    d.addErrback(log.msg)
    d.addCallback(batchLookup, client)
    return d


def dispatchQueries(client):
    # set up some test data and then set off the queries
    log.msg("Preparing to insert some data for querying ...")
    deferreds = [
        client.set("contractor:dev:part-time", "john"),
        client.set("contractor:admin:full-time", "carole"),
        client.set("exec:cto:local", "jane"),
        client.set("employee:admin:local", "john"),
        client.set("employee:security:local", "alice"),
        client.set("employee:finance:local", "bob"),
        client.set("employee:dev:remote", "dave"),
        client.set("employee:dev:local", "eve")]
    d = defer.DeferredList(deferreds)
    d.addErrback(log.err)
    d.addCallback(doEmployeeQuery, client)
    d.addCallback(doContractorQuery, client)
    d.addCallback(doEmployeeDevQuery, client)
    d.addCallback(doEmployeeLocalQuery, client)
    return d


def finish(ignore):
    reactor.stop()


def example():
    endpoint = TCP4ClientEndpoint(reactor, "127.0.0.1", 6379)
    d = endpoint.connect(RedisClientFactory())
    d.addErrback(log.err)
    d.addCallback(dispatchQueries)
    d.addCallback(finish)
    return d


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    example()
    reactor.run()