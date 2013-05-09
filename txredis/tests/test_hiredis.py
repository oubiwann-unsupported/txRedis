# if hiredis and its python wrappers are installed, test them too
try:
    import hiredis
    isHiRedis = True

except ImportError:
    isHiRedis = False

from txredis.client import HiRedisClient
from txredis.tests.client import (
    test_general, test_string, test_list, test_hash, test_set)


if isHiRedis:

    class HiRedisGeneral(test_general.GeneralCommandTestCase):

        protcol = HiRedisClient


    class HiRedisStrings(test_string.StringsCommandTestCase):

        protocol = HiRedisClient


    class HiRedisLists(test_list.ListsCommandsTestCase):

        protocol = HiRedisClient


    class HiRedisHash(test_hash.HashCommandsTestCase):

        protocol = HiRedisClient


    class HiRedisSortedSet(test_set.SortedSetCommandsTestCase):

        protocol = HiRedisClient


    class HiRedisSets(test_set.SetsCommandsTestCase):

        protocol = HiRedisClient


    _hush_pyflakes = hiredis
    del _hush_pyflakes