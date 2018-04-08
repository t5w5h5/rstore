# rstore: General purpose store for key/value pairs, time series and event sequences with optional encryption.
# Copyright: (c) 2018, t5w5h5@gmail.com. All rights reserved.
# License: MIT, see LICENSE for details.

import redis as _redis
import uuid as _uuid


class RedisStore:
    """Redis backend for Store."""

    def __init__(self, dsn, namespace):
        self._dsn = dsn
        self._namespace = namespace
        self._redis = _redis.from_url(self._dsn)

    def enter__(self, frozen):
        """Connect to redis server."""
        self._redis.time()

    def exit__(self, exc_type, exc_val, exc_tb):
        """Make sure the store is closed."""

    def discard__(self):
        """Delete the entire namespace from the store."""
        if self.kv_keys():
            self.kv_delete(*self.kv_keys())
        for hk in self.ts_keys():
            self.ts_delete(hk, '-inf', '+inf')
        for hk in self.es_keys():
            del_events = []
            for t, rstate in self.es_events(hk, start=None, end=None, raw_ops=None):
                del_events.extend([(t, item, *rov) for item, rov in rstate.items()])
            self.es_delete(hk, del_events)

    ##########  KEY / VALUE  #################################################

    def kv_get(self, ns_key):
        """Return raw value from store, or None if key not contained in store."""
        return self._redis.get('kv'+ns_key)

    def kv_set(self, ns_key, raw_value):
        """Store value."""
        self._redis.set('kv'+ns_key, raw_value)

    def kv_delete(self, *ns_keys):
        """Delete value(s) from store."""
        self._redis.delete(*('kv'+_ for _ in ns_keys))

    def kv_keys(self):
        """Return [kv keys]."""
        return [str(_[2:], encoding='utf8') for _ in self._redis.keys(f'kv{self._namespace}__*')]


    ##########  TIME SERIES  #################################################

    def ts_range(self, ns_key, start, end):
        """Return [(timestamp,raw_value)] between *start* and *end* time."""
        return [(_[1], _[0][10:]) for _ in self._redis.zrangebyscore('ts'+ns_key, min=start, max=end, withscores=True)]

    def ts_first(self, ns_key):
        """Return timestamp of oldest data point, or None if key is unknown."""
        r = self._redis.zrange('ts'+ns_key, 0, 0, withscores=True)
        return None if not r else r[0][1]

    def ts_last(self, ns_key):
        """Return timestamp of most recent data point, or None if key is unknown."""
        r = self._redis.zrange('ts'+ns_key, -1, -1, withscores=True)
        return None if not r else r[0][1]

    def ts_extend(self, ns_key, raw_data_points):
        """Add/replace [(timestamp,raw value)] to time series."""
        for ts, rv in raw_data_points:
            # Add the byte-encoded timestamp to the value to make sure that the value is unique within the set.
            self._redis.zadd('ts'+ns_key, bytes(f'{ts:010}', encoding='ascii')+rv, ts)

    def ts_delete(self, ns_key, start, end):
        """Delete data points between *start* and *end* time."""
        self._redis.zremrangebyscore('ts'+ns_key, min=start, max=end)

    def ts_keys(self):
        """Return [ts keys]."""
        return [str(_[2:], encoding='utf8') for _ in self._redis.keys(f'ts{self._namespace}__*')]


    ##########  EVENT SEQUENCES  #############################################

    def es_events(self, ns_key, start, end, raw_ops):
        """Return [(timestamp,{item:(op,value)})] sorted by ascending timestamp, optionally filtered by time and ops."""
        raw_events, tstate = [], None
        for row_id, t in self._redis.zrangebyscore('es'+ns_key, min=start or '-inf', max=end or '+inf', withscores=True):
            row = self._redis.hmget(row_id, ['item', 'op', 'value'])
            item, ro, rv = str(row[0], encoding='utf8'), row[1], row[2]
            if raw_ops and ro not in raw_ops:
                continue
            if t != tstate:
                if tstate is not None:
                    raw_events.append((tstate, rstate))
                tstate, rstate = t, {}
            rstate[item] = (ro, rv)
        if tstate is not None:
            raw_events.append((tstate, rstate))
        return raw_events

    def es_apply(self, ns_key, timestamp, raw_state):
        """Store raw_state {item:(raw_op,raw_value)}."""
        if self._redis.zrangebyscore('es'+ns_key, min=timestamp, max=timestamp):
            raise KeyError(timestamp)
        with self._redis.pipeline() as pipe:
            for item, raw_ov in raw_state.items():
                row_id = _uuid.uuid4()
                pipe.hmset(row_id, {'item': item, 'op': raw_ov[0], 'value': raw_ov[1]})
                pipe.zadd('es'+ns_key, row_id, timestamp)
            pipe.execute()

    def es_delete(self, ns_key, raw_events):
        """Delete [(timestamp,item,raw_op,_)]."""
        with self._redis.pipeline() as pipe:
            for row_id, t in self._redis.zrangebyscore('es'+ns_key, min='-inf', max='+inf', withscores=True):
                row = self._redis.hmget(row_id, ['item', 'op'])
                item, ro = str(row[0], encoding='utf8'), row[1]
                for re in raw_events:
                    if re[0] == t and re[1] == item and re[2] == ro:
                        pipe.delete(row_id)
                        pipe.zrem('es'+ns_key, row_id)
                        break
            pipe.execute()
        if self._redis.zcard('es'+ns_key) == 0:
            self._redis.delete('es'+ns_key)

    def es_keys(self):
        """Return [es keys]."""
        return [str(_[2:], encoding='utf8') for _ in self._redis.keys(f'es{self._namespace}__*')]
