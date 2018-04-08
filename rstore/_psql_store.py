# rstore: General purpose store for key/value pairs, time series and event sequences with optional encryption.
# Copyright: (c) 2018, t5w5h5@gmail.com. All rights reserved.
# License: MIT, see LICENSE for details.

from contextlib import closing as _closing
import psycopg2 as _psycopg2


class PsqlStore:
    """PostgreSQL backend for Store."""

    def __init__(self, dsn, namespace):
        self._dsn = dsn
        self._namespace = namespace
        self._conn = None

    def enter__(self, frozen):
        """Connect to database. In write mode, the store schema will be created if necessary."""
        self._conn = _psycopg2.connect(self._dsn)

        if not frozen:
            try:
                self._conn.cursor().execute(f'CREATE SCHEMA {self._namespace}')
            except _psycopg2.ProgrammingError:
                self._conn.rollback()     # Schema already exists

        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name=%s", (self._namespace,))
            row = c.fetchone()
            if row is None:
                raise FileNotFoundError((self._dsn, self._namespace))
            c.execute(f'SET search_path TO {self._namespace}')

        if not frozen:
            with _closing(self._conn.cursor()) as c:
                c.execute(
                    "CREATE TABLE IF NOT EXISTS _kv ("
                        "_key TEXT NOT NULL PRIMARY KEY,"
                        "_value BYTEA NOT NULL)")
                c.execute(
                    "CREATE TABLE IF NOT EXISTS _ts ("
                        "_key TEXT NOT NULL,"
                        "_timestamp BIGINT NOT NULL,"
                        "_value BYTEA NOT NULL,"
                        "PRIMARY KEY (_key, _timestamp))")
                c.execute(
                    "CREATE TABLE IF NOT EXISTS _es ("
                        "_key TEXT NOT NULL,"
                        "_timestamp BIGINT NOT NULL,"
                        "_item TEXT NOT NULL,"
                        "_op BYTEA NOT NULL,"
                        "_value BYTEA NOT NULL,"
                        "PRIMARY KEY (_key, _timestamp, _item, _op))")
        self._conn.commit()

    def exit__(self, exc_type, exc_val, exc_tb):
        """Make sure the store is closed."""
        if self._conn:
            self._conn.close()

    def discard__(self):
        """Delete the entire namespace from the store."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"DROP SCHEMA {self._namespace} CASCADE")
        self._conn.commit()


    ##########  KEY / VALUE  #################################################

    def kv_get(self, ns_key):
        """Return raw value from store, or None if key not contained in store."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT _value FROM _kv WHERE _key=%s", (ns_key,))
            vrow = c.fetchone()
            return None if vrow is None else bytes(vrow[0])

    def kv_set(self, ns_key, raw_value):
        """Store value."""
        with _closing(self._conn.cursor()) as c:
            try:
                c.execute("INSERT INTO _kv VALUES (%s,%s)", (ns_key, raw_value))
            except _psycopg2.IntegrityError:
                self._conn.rollback()
                c.execute("UPDATE _kv SET _value=%s WHERE _key=%s", (raw_value, ns_key))
        self._conn.commit()

    def kv_delete(self, *ns_keys):
        """Delete value(s) from store."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"DELETE FROM _kv WHERE _key IN ({','.join(['%s']*len(ns_keys))})", ns_keys)
        self._conn.commit()

    def kv_keys(self):
        """Return [kv keys]."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"SELECT _key FROM _kv WHERE _key LIKE '{self._namespace}__%'")
            return [_[0] for _ in c.fetchall()]


    ##########  TIME SERIES  #################################################

    def ts_range(self, ns_key, start, end):
        """Return [(timestamp,raw value,{})] between *start* and *end* time."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT _timestamp, _value FROM _ts WHERE _key=%s AND _timestamp>=%s AND _timestamp<=%s", (ns_key, start, end))
            raw_range = []
            for t, rv in c.fetchall():
                raw_range.append((t, bytes(rv)))
            return raw_range

    def ts_first(self, ns_key):
        """Return timestamp of oldest data point, or None if key is unknown."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT MIN(_timestamp) FROM _ts WHERE _key=%s", (ns_key,))
            t_min = c.fetchone()
            return None if t_min is None else t_min[0]

    def ts_last(self, ns_key):
        """Return timestamp of most recent data point, or None if key is unknown."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT MAX(_timestamp) FROM _ts WHERE _key=%s", (ns_key,))
            t_max = c.fetchone()
            return None if t_max is None else t_max[0]

    def ts_extend(self, ns_key, raw_data_points):
        """Add/replace [(timestamp,raw value)] to time series."""
        with _closing(self._conn.cursor()) as c:
            for t, rv in raw_data_points:
                try:
                    c.execute("INSERT INTO _ts VALUES (%s,%s,%s)", (ns_key, t, rv))
                except _psycopg2.IntegrityError:
                    self._conn.rollback()
                    c.execute("UPDATE _ts SET _value=%s WHERE _key=%s AND _timestamp=%s", (rv, ns_key, t))
        self._conn.commit()

    def ts_delete(self, ns_key, start, end):
        """Delete data points between *start* and *end* time."""
        with _closing(self._conn.cursor()) as c:
            c.execute(
                "DELETE FROM _ts WHERE _key=%s AND _timestamp>=%s AND _timestamp<=%s",
                (ns_key, start, end))
        self._conn.commit()

    def ts_keys(self):
        """Return [ts keys]."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"SELECT DISTINCT _key FROM _ts WHERE _key LIKE '{self._namespace}__%'")
            return [_[0] for _ in c.fetchall()]


    ##########  EVENT SEQUENCES  #############################################

    def es_events(self, ns_key, start, end, raw_ops):
        """Return [(timestamp,{item:(op,value)})] sorted by ascending timestamp,
        optionally filtered by time and ops."""
        with _closing(self._conn.cursor()) as c:
            conds, condv = ['_key=%s'], [ns_key]
            if start is not None:
                conds.append('_timestamp>=%s')
                condv.append(start)
            if end is not None:
                conds.append('_timestamp<=%s')
                condv.append(end)
            if raw_ops:
                conds.append(f"_op IN ({','.join(['%s']*len(raw_ops))})")
                condv.extend(raw_ops)
            wheres = '' if not conds else ' WHERE ' + ' AND '.join(conds)
            c.execute("SELECT _timestamp, _item, _op, _value FROM _es " + wheres + " ORDER BY _timestamp ASC", condv)
            events, tstate = [], None

            def append_event(tstate, rstate):
                events.append((tstate, rstate))

            for erow in c.fetchall():
                t, item, ro, rv = erow[0], erow[1], bytes(erow[2]), bytes(erow[3])
                if t != tstate:
                    if tstate is not None:
                        append_event(tstate, rstate)
                    tstate, rstate = t, {}
                rstate[item] = (ro, rv)

            if tstate is not None:
                append_event(tstate, rstate)

            return events

    def es_apply(self, ns_key, timestamp, raw_state):
        """Store raw_state {item:(raw_op,raw_value)}."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT COUNT(*) FROM _es WHERE _key=%s AND _timestamp=%s", (ns_key, timestamp))
            if c.fetchone()[0] > 0:
                raise KeyError(timestamp)
            for item, raw_ov in raw_state.items():
                c.execute("INSERT INTO _es VALUES (%s,%s,%s,%s,%s)", (ns_key, timestamp, item, *raw_ov))
        self._conn.commit()

    def es_delete(self, ns_key, raw_events):
        """Delete [(timestamp,item,raw_op,_)]."""
        with _closing(self._conn.cursor()) as c:
            for re in raw_events:
                c.execute("DELETE FROM _es WHERE _key=%s AND _timestamp=%s AND _item=%s AND _op=%s", (ns_key, re[0], re[1], re[2]))
        self._conn.commit()

    def es_keys(self):
        """Return [es keys]."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"SELECT DISTINCT _key FROM _es WHERE _key LIKE '{self._namespace}__%'")
            return [_[0] for _ in c.fetchall()]
