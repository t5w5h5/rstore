# rstore: General purpose store for key/value pairs, time series and event sequences with optional encryption.
# Copyright: (c) 2018, t5w5h5@gmail.com. All rights reserved.
# License: MIT, see LICENSE for details.


import operator as _operator
import pickle as _pickle
import re as _re
import time as _time

try:
    from cryptography import fernet as _fernet
    _crypto_ready = True
except ImportError:
    _crypto_ready = False

try:
    from ._psql_store import PsqlStore
    _pstore_available = True
except ImportError:
    _pstore_available = False

try:
    from ._redis_store import RedisStore
    _rstore_available = True
except ImportError:
    _rstore_available = False

from ._sqlite_store import SqliteStore

__all__ = ['StoreError', 'FrozenStoreError', 'StoreNotAvailableError', 'StoreIntegrityError', 'Store']


class StoreError(Exception):
    """Base error."""


class FrozenStoreError(StoreError):
    """Cannot write to read-only store."""


class StoreNotAvailableError(StoreError):
    """Cannot find or connect to store."""


class StoreIntegrityError(StoreError):
    """Cannot write to store due to integrity violation."""


class Store:
    """General purpose store for key/value pairs, time series and event sequences with optional encryption."""

    _namespace = None   # Fixed namespace for the store.
    _key = None         # Key for encryption.

    def __init__(self, dsn, namespace='default', key=None, frozen=True):
        assert key is None or _crypto_ready, "'cryptography' package required for encrypted stores"
        if dsn.startswith('postgresql://'):
            assert _pstore_available, "'psycopg2' package required for PostgreSQL backend"
            be = PsqlStore
        elif dsn.startswith('redis://'):
            assert _rstore_available, "'redis' package required for Redis backend"
            be = RedisStore
        elif dsn.startswith('sqlite://'):
            be = SqliteStore
            dsn = dsn[9:]
        else:
            assert False, ('Invalid dsn', dsn)
        ns = namespace or self._namespace
        self._namespace = '__' + ns.replace('.', '__')
        self._key = key or self._key
        self._frozen = frozen
        self._backend = be(dsn, self._namespace)

    def __enter__(self):
        """Return Store after opening it."""
        try:
            self._backend.enter__(self._frozen)
            return self
        except Exception as ex:
            raise StoreNotAvailableError(ex)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Make sure the store is closed."""
        self._backend.exit__(exc_type, exc_val, exc_tb)

    def discard(self):
        """Delete the entire namespace from the store."""
        if self._frozen:
            raise FrozenStoreError
        self._backend.discard__()

    def _ns_key(self, key):
        """Return key with namespace prefix."""
        assert key is not None and isinstance(key, str)
        return f'{self._namespace}__{key}'

    def _un_ns_key(self, ns_key):
        """Return key without namespace prefix."""
        k = ns_key.rsplit('__', maxsplit=1)
        assert len(k) == 2 and k[0] == self._namespace, ('Key with unexpected syntax', ns_key)
        return k[1]

    def _dump(self, value):
        """Return serialized data from value."""
        return _pickle.dumps(value, protocol=4)

    def _value_to_store(self, value, encrypt=True):
        """Return store data from value."""
        _ = self._dump(value)
        return _ if self._key is None or encrypt is False else _fernet.Fernet(self._key).encrypt(_)

    def _load(self, data):
        """Return deserialized value from data."""
        return _pickle.loads(data)

    def _value_from_store(self, data, decrypt=True):
        """Return value from store data."""
        return self._load(data if self._key is None or decrypt is False else _fernet.Fernet(self._key).decrypt(data))


    ##########  KEY / VALUE  #################################################

    def get(self, key, default=None):
        """Return value from store. Return default if key not contained in store."""
        raw_value = self._backend.kv_get(self._ns_key(key))
        if raw_value is None:
            return default
        return self._value_from_store(raw_value)

    def set(self, key, value):
        """Store value."""
        if self._frozen:
            raise FrozenStoreError
        self._backend.kv_set(self._ns_key(key), self._value_to_store(value))

    def delete(self, *keys):
        """Delete key(s) from store."""
        if self._frozen:
            raise FrozenStoreError
        if len(keys):
            self._backend.kv_delete(*(self._ns_key(_) for _ in keys))

    def keys(self):
        """Return [kv keys]."""
        return [self._un_ns_key(_) for _ in self._backend.kv_keys()]

    def has(self, key):
        """Return True if k/v store contains key."""
        return self._un_ns_key(self._ns_key(key)) in self.keys()

    def __getitem__(self, key):
        """Return value from store. Raise KeyError if key not contained in store."""
        v = self.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __contains__(self, key):
        return self.has(key)

    def __len__(self):
        return len(self.keys())

    def find(self, *pattern):
        """Return [(key,value)] that match key pattern. Return all values if pattern is None.
        Pattern can contain the wildcard characters '*' and '?'.
        """
        keys = self.keys()

        if pattern:
            # Pre-determine set of matching keys.
            pattern = pattern[0].replace('.', '\.').replace('*', '.*')
            keys = [k for k in keys if _re.match(pattern, k)]

        return [(k, self.get(k)) for k in keys]


    ##########  TIME SERIES  #################################################

    _freq = None     # Default frequency for time series data points.

    def range(self, key, start=None, *, n=None, end=None, freq=None):
        """Return [(timestamp,value)] between *start* and *end* time.
        *start* defaults to first available data point, *end* defaults to last available data point.
        If *n* is given, maximum n data points are returned from *start*, or up to *end* whichever is reached first."""

        # Retrieve raw data points from backend.
        if start is None:
            first = self.first(key)
            if first is None:
                return []
            start = first[0]
        if end is None:
            last = self.last(key)
            if last is None:
                return []
            end = last[0]
        if end < start:
            return []
        raw_range = self._backend.ts_range(self._ns_key(key), start, end)

        # Filter data points according to *n* and *freq*.
        found = []
        freq, step = freq or self._freq, 0
        for t, rv in raw_range:
            if t >= step:
                found.append((t, self._value_from_store(rv)))
                if n is not None and len(found) == n:
                    break
                if freq is not None:
                    step = t + freq

        return found

    def first(self, key):
        """Return (timestamp,value) of oldest data point, or None if key is unknown."""
        t_min = self._backend.ts_first(self._ns_key(key))
        return None if t_min is None else self.range(key, start=t_min, end=t_min)[0]

    def last(self, key):
        """Return (timestamp,value) of most recent data point, or None if key is unknown."""
        t_max = self._backend.ts_last(self._ns_key(key))
        return None if t_max is None else self.range(key, start=t_max, end=t_max)[0]

    def missing(self, key, start, end=None, freq=None):
        """Return [timestamp] of missing data points between *start* and *end* time. End defaults to start time."""
        end, freq = end or start, freq or self._freq
        assert freq is not None, 'Cannot determine missing data points without known frequency.'
        timestamps = set(map(_operator.itemgetter(0), self.range(key, start, end=end, freq=freq)))
        return [ts for ts in range(start, end+freq, freq) if ts not in timestamps]

    def iscomplete(self, key, start, end=None, freq=None):
        """Return True if there are no missing data points between *start* and *end* time.
        End defaults to start time."""
        return not self.missing(key, start, end, freq or self._freq)

    def extend(self, key, *data_points):
        """Add/replace (timestamp,value) data points to time series."""
        if self._frozen:
            raise FrozenStoreError
        self._backend.ts_extend(self._ns_key(key), [(dp[0], self._value_to_store(dp[1])) for dp in data_points])

    def remove(self, *keys, start=None, end=None):
        """Delete data points from time series for one or multiple keys. Start defaults to first available data point,
        end defaults to last available data point."""
        if self._frozen:
            raise FrozenStoreError
        for key in keys:
            kstart, kend = start, end
            if kstart is None:
                first = self.first(key)
                if first is None:
                    continue
                kstart = first[0]
            if kend is None:
                last = self.last(key)
                if last is None:
                    continue
                kend = last[0]
            if kend < kstart:
                continue
            self._backend.ts_delete(self._ns_key(key), kstart, kend)

    def timeseries(self):
        """Return [ts keys]."""
        return [self._un_ns_key(_) for _ in self._backend.ts_keys()]


    ##########  EVENT SEQUENCES  #############################################

    def current(self, key, withtimestamp=False):
        """Return current state {item:value}|(timestamp,{item:value}}, or None if key is unknown."""
        return self.past(key, t=None, withtimestamp=withtimestamp)

    def past(self, key, t, withtimestamp=False):
        """Return state {item:value}|(timestamp,{item:value}} as it existed at time *t*,
        or None if key was unknown at that time."""
        events = self.events(key, start=None, end=t)
        if not events:
            return None
        state = {}
        for t, s_change in events:
            for item, (op, value) in s_change.items():
                if op in {_operator.setitem}:
                    op(state, item, value)
                elif op in {_operator.delitem}:
                    op(state, item)
                elif op in {_operator.add, _operator.concat, _operator.floordiv, _operator.mod, _operator.mul, _operator.pow, _operator.sub, _operator.truediv}:
                    state[item] = op(state[item], value)
                elif op in {_operator.neg, _operator.not_}:
                    state[item] = op(state[item])
                else:
                    assert False, ('Invalid operator:', op)
        return (t, state) if withtimestamp else state

    def apply(self, key, state, t=None):
        """Return current state {item:value} after applying *state* {item:value|(op,value)} at time *t*.
        Raise StoreIntegrityError if attempting to overwrite an existing state."""
        raw_state = {}
        for item, ov in state.items():
            assert isinstance(item, str)
            if not isinstance(ov, tuple):
                ov = _operator.setitem, ov
            raw_state[item] = (_pickle.dumps(ov[0], protocol=4), self._value_to_store(ov[1]))
        try:
            self._backend.es_apply(self._ns_key(key), t or int(round(_time.time() * 1000)), raw_state)
        except KeyError:
            raise StoreIntegrityError((key, t))
        return self.current(key)

    def prune(self, *keys, remove=True):
        """Prune obsolete events that do not directly contribute to the current state. Remove key completely if
        current state is empty (i.e. all items have been deleted.)"""
        for key in keys:

            # Delete empty state entirely if *remove* is True.
            if not self.current(key) and remove is True:
                del_events = []
                for t, rstate in self._backend.es_events(self._ns_key(key), start=None, end=None, raw_ops=None):
                    del_events.extend([(t, item, *rov) for item, rov in rstate.items()])
                self._backend.es_delete(self._ns_key(key), del_events)
                continue

            # TODO Pruning of operations that no longer modify the current state.

    def events(self, key, start=None, end=None, ops=None):
        """Return [(timestamp,{item:(op,value)})] sorted by ascending timestamp, optionally filtered by time, and ops.
        Return None is key is unknown."""
        if self._un_ns_key(self._ns_key(key)) not in self.eventsequences():
            return None
        if ops is not None:
            if not isinstance(ops, (list, set, tuple)):
                ops = [ops]
            ops = [_pickle.dumps(_, protocol=4) for _ in ops]

        found = []
        for t, rstate in self._backend.es_events(self._ns_key(key), start, end, ops or []):
            found.append((t, {item: (_pickle.loads(rov[0]), self._value_from_store(rov[1])) for item, rov in rstate.items()}))

        return found

    def eventsequences(self):
        """Return [es keys]."""
        return [self._un_ns_key(_) for _ in self._backend.es_keys()]
