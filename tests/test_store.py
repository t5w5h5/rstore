# rstore: General purpose store for key/value pairs, time series and event sequences with optional encryption.
# Copyright: (c) 2018, t5w5h5@gmail.com. All rights reserved.
# License: MIT, see LICENSE for details.

import json
import operator
import time
import unittest

try:
    from cryptography.fernet import Fernet
    test_crypto = True
except ImportError:
    test_crypto = False


from rstore import *


class _RegularSetup:

    dsn = None

    def setUp(self):
        self.read_store = Store(self.dsn)
        self.write_store = Store(self.dsn, frozen=False)
        self.write_store._freq = 10

    def tearDown(self):
        with self.write_store as s:
            s.discard()


class _RegularKV:

    def test_kv_set_get_has_delete(self):
        # Exception when opening non-existing store on frozen mode.
        if not self.dsn.startswith('redis://'):
            with self.assertRaises(StoreNotAvailableError):
                with self.read_store:
                    pass

        # Okay to open non-existing store in read-write mode.
        with self.write_store as s:
            s.set('test', 'Some str')
            self.assertEqual(s.get('test'), 'Some str')

        with self.assertRaises(FrozenStoreError):
            with self.read_store as s:
                s.set('test', 'Some str')

        with self.read_store as s:
            self.assertEqual(s.get('test'), 'Some str')
            self.assertTrue(s.has('test'))
            self.assertEqual(s.keys(), ['test'])

        with self.write_store as s:
            s.delete('test')
            self.assertFalse(s.has('test'))
            self.assertIsNone(s.get('test'))
            self.assertEqual(s.get('test', 'Default'), 'Default')

    def test_kv_magic(self):
        with self.write_store as s:
            self.assertEqual(len(s), 0)
            self.assertFalse('test' in s)

            s['test'] = 'Some str'
            self.assertEqual(len(s), 1)
            self.assertTrue('test' in s)
            self.assertEqual(s['test'], 'Some str')

            del s['test']
            self.assertFalse('test' in s)

            with self.assertRaises(KeyError):
                s['test']

    def test_kv_find(self):
        with self.write_store as s:
            s.set('test.key1', 0)
            s.set('test.key1', 1)
            self.assertTrue(s.has('test.key1'))
            self.assertFalse(s.has('test.key2'))

            s.set('test.key2', 2)
            self.assertEqual(sorted(s.keys()), ['test.key1', 'test.key2'])
            self.assertEqual(s.get('test.key2'), 2)

            s.set('key3.test.abcd', 3)
            self.assertEqual(sorted(s.find()), [('key3.test.abcd', 3), ('test.key1', 1), ('test.key2', 2)])
            self.assertEqual(sorted(s.find('test.*')), [('test.key1', 1), ('test.key2', 2)])
            self.assertEqual(s.find('test.abc*'), [])
            self.assertEqual(len(s), 3)


class _RegularTS:

    def test_ts_all(self):
        with self.write_store as s:
            self.assertEqual(s.timeseries(), [])
            self.assertEqual(s.range('test', 100, end=150), [])
            self.assertIsNone(s.first('test'))
            self.assertIsNone(s.last('test'))
            self.assertEqual(s.missing('test', 100, 150), [100, 110, 120, 130, 140, 150])
            self.assertFalse(s.iscomplete('test', 100, 150))

            s.extend('test', (100, '0'))
            self.assertEqual(s.first('test'), (100, '0'))
            self.assertEqual(s.first('test'), s.last('test'))

            s.extend('test', (110, '1'), (120, '2'), (130, '3'), (140, '4'), (150, '5'))
            self.assertEqual(s.first('test'), (100, '0'))
            self.assertEqual(s.last('test'), (150, '5'))
            self.assertEqual(s.missing('test', 100, 150), [])
            self.assertTrue(not s.missing('test', 120))
            self.assertTrue(s.iscomplete('test', 100, 150))

            self.assertEqual(s.range('test', n=2), [(100, '0'), (110, '1')])
            self.assertEqual(s.range('test', 130, n=1, end=150), [(130, '3')])
            self.assertEqual(s.range('test', 130), [(130, '3'), (140, '4'), (150, '5')])
            self.assertEqual(s.range('test', 125, end=145), [(130, '3'), (140, '4')])

            s.remove('test', start=120, end=120)
            self.assertEqual(s.missing('test', 100, 150), [120])
            self.assertFalse(not s.missing('test', 120))
            self.assertFalse(s.iscomplete('test', 100, 150))

            s.remove('test')
            self.assertEqual(s.range('test'), [])
            self.assertEqual(s.timeseries(), [])

    # def test_ts_speed(self):
    #
    #     # Regular: 21s for SQLite, 3m 2s for PSQL, 2m 24s for Redis
    #     # Encrypted: 2m 36s for SQLite, 5m 50s for PSQL, 4m 44s for Redis
    #     with self.write_store as s:
    #         self.assertEqual(s.timeseries(), [])
    #         s.extend('test', *[(ts, 'abcdefg') for ts in range(1000000)])
    #         self.assertEqual(len(s.range('test')), 100000)  # Because of freq=10
    #         s.remove('test')
    #         self.assertEqual(s.range('test'), [])
    #         self.assertEqual(s.timeseries(), [])


class _RegularES:

    def test_es_all(self):
        ts = int(round(time.time() * 1000))
        with self.write_store as s:
            self.assertEqual(s.eventsequences(), [])
            self.assertIsNone(s.current('test'))
            self.assertIsNone(s.past('test', ts))
            self.assertIsNone(s.events('test'))

            self.assertIsNone(s.apply('test', {}))   # 0 events

            state = s.apply('test', {'int_item': 1, 'str_item': 'Hello', 'bool_item': False})   # 1 events
            self.assertEqual(state, {'int_item': 1, 'str_item': 'Hello', 'bool_item': False})
            self.assertEqual(s.current('test'), state)
            print(s.current('test', withtimestamp=True))

            ts = int(round(time.time() * 1000))
            time.sleep(0.1)

            self.assertEqual(s.apply('test', {'str_item': (operator.delitem, None)}), {'int_item': 1, 'bool_item': False})  # 1 event
            self.assertEqual(s.current('test'), {'int_item': 1, 'bool_item': False})
            self.assertEqual(s.past('test', ts), {'int_item': 1, 'str_item': 'Hello', 'bool_item': False})

            self.assertEqual(s.apply('test', {'bool_item': (operator.not_, None)}), {'int_item': 1, 'bool_item': True})    # 1 event
            self.assertEqual(s.apply('test', {'bool_item': (operator.not_, None)}), {'int_item': 1, 'bool_item': False})   # 1 event

            self.assertEqual(s.apply('test', {'int_item': (operator.add, 10)}), {'int_item': 11, 'bool_item': False})       # 1 event
            self.assertEqual(s.apply('test', {'int_item': (operator.neg, None)}), {'int_item': -11, 'bool_item': False})    # 1 event

            self.assertEqual(s.apply('test', {}), {'int_item': -11, 'bool_item': False})     # 0 event
            self.assertEqual(s.apply('test', {'int_item': 10}), {'int_item': 10, 'bool_item': False})       # 1 event

            self.assertEqual(len(s.events('test')), 7)
            self.assertEqual(len(s.events('test', start=ts)), 6)
            self.assertEqual(len(s.events('test', start=ts, end=ts)), 0)
            self.assertEqual(len(s.events('test', end=int(round(time.time() * 1000)))), 7)
            self.assertEqual(len(s.events('test', ops=[operator.not_])), 2)

            self.assertEqual(s.apply('test', {'str_item': 'Ho Ho Ho'}), {'int_item': 10, 'bool_item': False, 'str_item': 'Ho Ho Ho'})

            # Changing item in the past does no alter the current state since item was set afterwards.
            self.assertEqual(s.apply('test', {'str_item': 'Too old'}, ts), {'int_item': 10, 'bool_item': False, 'str_item': 'Ho Ho Ho'})

            # State cannot be changed twice at the same time.
            with self.assertRaises(StoreIntegrityError):
                s.apply('test', {'str_item': 'Too old'}, ts)

            # Adding item in the past alters the current state.
            self.assertEqual(s.apply('test', {'str2_item': 'Added'}, ts+1), {'int_item': 10, 'bool_item': False, 'str_item': 'Ho Ho Ho', 'str2_item': 'Added'})

            # Delete operator.
            self.assertEqual(s.apply('test', {'str_item': (operator.delitem, None), 'bool_item': (operator.delitem, None),
                                              'str2_item': (operator.delitem, None), 'int_item': (operator.delitem, None)}), {})

            s.prune('test', remove=True)
            self.assertIsNone(s.current('test'))


class PsqlStore_Regular(_RegularSetup, _RegularKV, _RegularTS, _RegularES, unittest.TestCase):
    dsn = 'postgresql://localhost'


class RedisStore_Regular(_RegularSetup, _RegularKV, _RegularTS, _RegularES, unittest.TestCase):
    dsn = 'redis://localhost'


class SqliteStore_Regular(_RegularSetup, _RegularKV, _RegularTS, _RegularES, unittest.TestCase):
    dsn = 'sqlite://unittest.store'


class _EncryptedSetup:

    dsn = None

    def setUp(self):
        if not test_crypto:
            raise RuntimeError('Cannot test crypto without "cryptography" package')
        key = Fernet.generate_key()
        self.read_store = Store(self.dsn, key=key)
        self.write_store = Store(self.dsn, key=key, frozen=False)
        self.write_store._freq = 10

    def tearDown(self):
        with self.write_store as s:
            s.discard()


class _EncryptedKV:

    def test_kv_set_get_has_delete(self):
        # Exception when opening non-existing store on read-only mode.
        if not self.dsn.startswith('redis://'):
            with self.assertRaises(StoreNotAvailableError):
                with self.read_store:
                    pass

        # Okay to open non-existing store in read-write mode.
        with self.write_store as s:
            s.set('test', 'Some str')
            self.assertEqual(s.get('test'), 'Some str')

        with self.assertRaises(FrozenStoreError):
            with self.read_store as s:
                s.set('test', 'Some str')

        with self.read_store as s:
            self.assertEqual(s.get('test'), 'Some str')
            self.assertTrue(s.has('test'))

        with self.write_store as s:
            s.set('unenc', 'Mixed mode')
            print(s.keys())

        with self.write_store as s:
            s.delete('test')
            self.assertFalse(s.has('test'))
            self.assertIsNone(s.get('test'))
            self.assertEqual(s.get('test', 'Default'), 'Default')


class PsqlStore_Encrypted(_EncryptedSetup, _EncryptedKV, _RegularTS, _RegularES, unittest.TestCase):
    dsn = 'postgresql://localhost'


class RedisStore_Encrypted(_EncryptedSetup, _EncryptedKV, _RegularTS, _RegularES, unittest.TestCase):
    dsn = 'redis://localhost'


class SqliteStore_Encrypted(_EncryptedSetup, _EncryptedKV, _RegularTS, _RegularES, unittest.TestCase):
    dsn = 'sqlite://unittest.store'


class _JsonSetup:

    dsn = None

    class JsonStore(Store):
        def _dump(self, value):
            return json.dumps(value)

        def _load(self, data):
            return json.loads(data)

    def setUp(self):
        self.test_store = _JsonSetup.JsonStore(self.dsn, frozen=False)

    def tearDown(self):
        with self.test_store as s:
            s.discard()

    def test_kv_set_get_has_delete(self):
        v = {'string': 'Some str', 'list': [1, 2, 3]}

        with self.test_store as s:
            s.set('test', v)
            self.assertEqual(s.get('test'), v)

            s['test'] = v
            self.assertEqual(s['test'], v)

        with self.test_store as s:
            self.assertEqual(s.get('test'), v)
            self.assertTrue(s.has('test'))
            self.assertEqual(s.keys(), ['test'])

        with self.test_store as s:
            s.delete('test')


class PsqlStore_Json(_JsonSetup, unittest.TestCase):
    dsn = 'postgresql://localhost'


class RedisStore_Json(_JsonSetup, unittest.TestCase):
    dsn = 'redis://localhost'


class SqliteStore_Json(_JsonSetup, unittest.TestCase):
    dsn = 'sqlite://unittest.store'
