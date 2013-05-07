##############################################
txRedis - asynchronous redis client for Python
##############################################

An asynchronous Redis client library for the `Twisted Python
networking framework`_.

Included are two protocol implementations, one using a custom Twisted
based protocol parser, and another using the `hiredis protocol parser`_.

To install txRedis and its dependencies (the Twisted and hiredis libraries),
just use `pip`::

  $ pip install .

Alternatively, you can install txRedis into a virtual environment::

  $ pip install virtualenv
  $ virtualenv .venv
  $ . .venv/bin/activate
  (.venv) $ pip install .

As above, this will install the txRedis dependencies.

If you want to run against the txRedis in your working directory, you can
uninstall txRedis; its dependencies will remain::

  (.venv $ pip uninstall txredis


Contact
=======

There is not a txredis list (yet) but questions can be raised on the
redis-client mailing list, or you can ask:

- Reza Lotun <rlotun@gmail.com>
- Dorian Raymer <deldotdr@gmail.com>


Bug tracker
===========

File bug reports and any other feedback with the `issue tracker`_.


.. Links
.. -----
.. _Twisted Python networking framework: http://www.twistedmatrix.com/
.. _hiredis protocol parser: https://github.com/pietern/hiredis-py
.. _issue tracker: http://github.com/deldotdr/txRedis/issues/
