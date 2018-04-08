# rstore: General purpose store for key/value pairs, time series and event sequences with optional encryption.
# Copyright: (c) 2018, t5w5h5@gmail.com. All rights reserved.
# License: MIT, see LICENSE for details.

from contextlib import closing as _closing
import os as _os
import sqlite3 as _sqlite3


class SqliteStore:
    """Sqlite3 backend for Store."""

    def __init__(self, filename, namespace):
        self._filename = filename
        self._namespace = namespace
        self._conn = None

    def enter__(self, frozen):
        """Open database. In write mode, the store file will be created if necessary.
        Raise FileNotFoundError in frozen mode if file does not exist."""
        if not _os.path.isfile(self._filename) and frozen:
            raise FileNotFoundError(self._filename)
        self._conn = _sqlite3.connect(self._filename)
        if not frozen:
            with _closing(self._conn.cursor()) as c:
                c.execute("PRAGMA foreign_keys = ON")
                c.execute(
                    "CREATE TABLE IF NOT EXISTS _kv ("
                        "_key TEXT NOT NULL PRIMARY KEY,"
                        "_value BLOB NOT NULL)")
                c.execute(
                    "CREATE TABLE IF NOT EXISTS _ts ("
                        "_key TEXT NOT NULL, "
                        "_timestamp BIGINT NOT NULL, "
                        "_value BLOB NOT NULL, "
                        "PRIMARY KEY (_key, _timestamp))")
                c.execute(
                    "CREATE TABLE IF NOT EXISTS _es ("
                        "_key TEXT NOT NULL,"
                        "_timestamp BIGINT NOT NULL,"
                        "_item TEXT NOT NULL,"
                        "_op BLOB NOT NULL,"
                        "_value BLOB NOT NULL,"
                        "PRIMARY KEY (_key, _timestamp, _item, _op))")

    def exit__(self, exc_type, exc_val, exc_tb):
        """Make sure the store is closed."""
        if self._conn:
            self._conn.close()

    def discard__(self):
        """Delete the entire namespace from the store. Remove file if database is empty."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"DELETE FROM _kv WHERE _key LIKE '{self._namespace}__%'")
            c.execute(f"DELETE FROM _ts WHERE _key LIKE '{self._namespace}__%'")
            c.execute(f"DELETE FROM _es WHERE _key LIKE '{self._namespace}__%'")
        self._conn.commit()

        # Delete file if store is completely empty.
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT COUNT(_key) FROM _kv")
            size = c.fetchone()[0]
            c.execute("SELECT COUNT(_key) FROM _ts")
            size += c.fetchone()[0]
            c.execute("SELECT COUNT(_key) FROM _es")
            size += c.fetchone()[0]
        if size == 0:
            self._conn.close()
            _os.remove(self._filename)


    ##########  KEY / VALUE  #################################################

    def kv_get(self, ns_key):
        """Return raw value from store, or None if key not contained in store."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT _value FROM _kv WHERE _key=?", (ns_key,))
            vrow = c.fetchone()
            return None if vrow is None else vrow[0]

    def kv_set(self, ns_key, raw_value):
        """Store value."""
        with _closing(self._conn.cursor()) as c:
            c.execute("INSERT OR REPLACE INTO _kv VALUES (?,?)", (ns_key, raw_value))
        self._conn.commit()

    def kv_delete(self, *ns_keys):
        """Delete value(s) from store."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"DELETE FROM _kv WHERE _key IN ({','.join('?'*len(ns_keys))})", ns_keys)
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
            c.execute("SELECT _timestamp, _value FROM _ts WHERE _key=? AND _timestamp>=? AND _timestamp<=?", (ns_key, start, end))
            raw_range = []
            for t, rv in c.fetchall():
                raw_range.append((t, rv))
            return raw_range

    def ts_first(self, ns_key):
        """Return timestamp of oldest data point, or None if key is unknown."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT MIN(_timestamp) FROM _ts WHERE _key=?", (ns_key,))
            t_min = c.fetchone()
            return None if t_min is None else t_min[0]

    def ts_last(self, ns_key):
        """Return timestamp of most recent data point, or None if key is unknown."""
        with _closing(self._conn.cursor()) as c:
            c.execute("SELECT MAX(_timestamp) FROM _ts WHERE _key=?", (ns_key,))
            t_max = c.fetchone()
            return None if t_max is None else t_max[0]

    def ts_extend(self, ns_key, raw_data_points):
        """Add/replace [(timestamp,raw value)] to time series."""
        with _closing(self._conn.cursor()) as c:
            for t, rv in raw_data_points:
                c.execute("INSERT OR REPLACE INTO _ts VALUES (?,?,?)", (ns_key, t, rv))
        self._conn.commit()

    def ts_delete(self, ns_key, start, end):
        """Delete data points between *start* and *end* time."""
        with _closing(self._conn.cursor()) as c:
            c.execute(
                "DELETE FROM _ts WHERE _key=? AND _timestamp>=? AND _timestamp<=?",
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
            conds, condv = ['_key=?'], [ns_key]
            if start is not None:
                conds.append('_timestamp>=?')
                condv.append(start)
            if end is not None:
                conds.append('_timestamp<=?')
                condv.append(end)
            if raw_ops:
                conds.append(f"_op IN ({','.join(['?']*len(raw_ops))})")
                condv.extend(raw_ops)
            wheres = '' if not conds else ' WHERE ' + ' AND '.join(conds)
            c.execute("SELECT _timestamp, _item, _op, _value FROM _es " + wheres + " ORDER BY _timestamp ASC", condv)
            events, tstate = [], None

            def append_event(tstate, rstate):
                events.append((tstate, rstate))

            for t, item, ro, rv in c.fetchall():
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
            c.execute("SELECT COUNT(*) FROM _es WHERE _key=? AND _timestamp=?", (ns_key, timestamp))
            if c.fetchone()[0] > 0:
                raise KeyError(timestamp)
            for item, raw_ov in raw_state.items():
                c.execute("INSERT INTO _es VALUES (?,?,?,?,?)", (ns_key, timestamp, item, *raw_ov))
        self._conn.commit()

    def es_delete(self, ns_key, raw_events):
        """Delete [(timestamp,item,raw_op,_)]."""
        with _closing(self._conn.cursor()) as c:
            for re in raw_events:
                c.execute("DELETE FROM _es WHERE _key=? AND _timestamp=? AND _item=? AND _op=?", (ns_key, re[0], re[1], re[2]))
        self._conn.commit()

    def es_keys(self):
        """Return [es keys]."""
        with _closing(self._conn.cursor()) as c:
            c.execute(f"SELECT DISTINCT _key FROM _es WHERE _key LIKE '{self._namespace}__%'")
            return [_[0] for _ in c.fetchall()]
