rstore
======

General purpose store for key/value pairs, time series and event sequences with optional encryption.

Values are encrypted as soon as the *key* argument is specified with ``Store()``. Alternatively, the key
can be fixed for the store by setting ``Store._key``. The same store can be used in encrypted and unencrypted mode.
*Note: Keys must be of type ``str`` and are never encrypted.*

Different backends are available for persistence. The backend is selected by the `dsn` argument of ``Store()``:

    - PostgreSQL database: ``dsn='postgresql://localhost'``
    - Redis server: ``dsn='redis://localhost'``
    - SQLite data file: ``dsn='sqlite://datafile.db'``

Different namespaces can be used within a single store. The namespace is selected with the *namespace* argument of
``Store()`` or by setting ``Store._namespace``. If no namespace is selected it defaults to *default*.

Values are stored using the pickle module (before encryption). This behavior can be changed by subclassing
``Store`` and overwriting ``_dump`` and ``_load``. For example:

.. code-block:: python

    class JsonStore(Store):
        def _dump(self, value):
            return json.dumps(value)
        def _load(self, data):
            return json.loads(data)

Key/value
---------

Functions for the key/value store include: ``set``, ``get``, ``delete``, ``has``, and ``keys``. The k/v store can
also be used in a dict-like fashion with ``s[k]``, ``del s[k]``, ``k in s``, and ``len(s)``.

.. code-block:: python

    with Store('file.name', frozen=False) as kv:
        kv.set('some_key', value)
        value = kv.get('some_key')
        del kv['some_key']

Time series
-----------

Time series are key/value pairs where the value changes over time. Each value change (or data point) is recorded
with a timestamp. The timestamp is an arbitrary *int* value and its meaning must be determined by the application.
An even frequency between the timestamps is optional.

Functions for time series include: ``range``, ``first``, ``last``, ``missing``, ``iscomplete``, ``extend``, ``remove``,
and ``timeseries``.

.. code-block:: python

    with Store('file.name', frozen=False) as ts:
        ts.extend('some_series', (timestamp1, value1), ...)
        timestamp, value = ts.last('some_series')
        ts.remove('some_series')

Event sequences
---------------

Event sequences are key/dict pairs where each change to the dict is recorded as an event.
Events are stored as tuple ``(timestamp, item, op, value)`` where:

- ``timestamp`` is the POSIX timestamp in milliseconds when the event was recorded (type ``int``)
- ``item`` is the name of the dict member (type ``str``)
- ``op`` is the operator to apply to the dict member (``operator.setitem``, ``operator.delitem``, ``operator.add``, ...)
- ``value`` is the value to apply to the dict member (or ``None`` for unary operators such as ``operator.delitem``)

Functions for event sequences include: ``current``, ``past``, ``apply``, ``prune``, ``events``, and ``eventsources``.

.. code-block:: python

    with Store('file.name', frozen=False) as es:
        es.apply('some_dict', {'item': 1})       # op defaults to operator.setitem
        es.apply('some_dict', {'item': (operator.add, 1)})
        es.events('some_dict') == [(timestamp, 'item', operator.setitem, 1), (timestamp, 'item', operator.add, 1)]
        es.current('some_dict') == {'item': 2}
        es.prune('some_dict', remove=True)

Installation
------------

.. code-block:: bash

    $ pip install rlib-store

Getting Started
---------------

.. code-block:: python

    from rstore import Store

Check the doc strings and unit tests for examples.

License
-------

"MIT". See LICENSE for details. Copyright t5w5h5@gmail.com, 2018.


