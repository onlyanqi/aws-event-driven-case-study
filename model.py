"""Sequential SQLite model of transaction boundaries, not an AWS emulator."""
import json
import sqlite3
import uuid


class InjectedFailure(RuntimeError):
    pass


class Lab:
    def __init__(self):
        self.db = sqlite3.connect(':memory:')
        self.db.executescript('''
            CREATE TABLE orders (id TEXT PRIMARY KEY, tenant TEXT, payload TEXT);
            CREATE TABLE requests (tenant TEXT, key TEXT, payload TEXT, order_id TEXT,
                                   PRIMARY KEY (tenant, key));
            CREATE TABLE outbox (id TEXT PRIMARY KEY, order_id TEXT, sent INTEGER DEFAULT 0);
            CREATE TABLE receipts (consumer TEXT, event_id TEXT, PRIMARY KEY (consumer, event_id));
            CREATE TABLE projection (order_id TEXT PRIMARY KEY, effects INTEGER);
        ''')
        self.published = []

    def count(self, table):
        if table not in {'orders', 'requests', 'outbox', 'receipts', 'projection'}:
            raise ValueError('Unknown table')
        return self.db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]

    def baseline(self):
        with self.db:
            self.db.execute('INSERT INTO orders VALUES (?, ?, ?)', ('lost-order', 'tenant', '{}'))
        raise InjectedFailure('Process stopped before SNS publish')

    def accept(self, tenant, key, payload, fail=False):
        encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        with self.db:
            old = self.db.execute('SELECT payload, order_id FROM requests WHERE tenant=? AND key=?',
                                  (tenant, key)).fetchone()
            if old:
                if old[0] != encoded:
                    raise ValueError('Idempotency key reused with different payload')
                return old[1]
            order_id, event_id = str(uuid.uuid4()), str(uuid.uuid4())
            self.db.execute('INSERT INTO orders VALUES (?, ?, ?)', (order_id, tenant, encoded))
            self.db.execute('INSERT INTO requests VALUES (?, ?, ?, ?)', (tenant, key, encoded, order_id))
            if fail:
                raise InjectedFailure('Transaction interrupted before outbox write')
            self.db.execute('INSERT INTO outbox (id, order_id) VALUES (?, ?)', (event_id, order_id))
        return order_id

    def relay(self, fail_before=False, fail_after=False):
        for event_id, order_id in self.db.execute('SELECT id, order_id FROM outbox WHERE sent=0').fetchall():
            if fail_before:
                raise InjectedFailure('SNS unavailable')
            self.published.append({'eventId': event_id, 'orderId': order_id})
            if fail_after:
                raise InjectedFailure('Publish succeeded; sent marker not committed')
            with self.db:
                self.db.execute('UPDATE outbox SET sent=1 WHERE id=?', (event_id,))

    def consume(self, event, fail=False, consumer='fulfillment'):
        with self.db:
            inserted = self.db.execute('INSERT OR IGNORE INTO receipts VALUES (?, ?)',
                                       (consumer, event['eventId'])).rowcount
            if not inserted:
                return 'duplicate'
            if fail:
                raise InjectedFailure('Stopped between receipt and projection')
            self.db.execute('''INSERT INTO projection VALUES (?, 1)
                ON CONFLICT(order_id) DO UPDATE SET effects=effects+1''', (event['orderId'],))
        return 'applied'

    def batch(self, messages, failing_ids=()):
        failures = []
        for message in messages:
            try:
                self.consume(message['body'], fail=message['messageId'] in failing_ids)
            except (InjectedFailure, KeyError, TypeError):
                failures.append({'itemIdentifier': message['messageId']})
        return {'batchItemFailures': failures}

    def effects(self):
        return self.db.execute('SELECT COALESCE(SUM(effects), 0) FROM projection').fetchone()[0]
