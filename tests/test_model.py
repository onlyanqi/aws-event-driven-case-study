import unittest
from model import Lab, InjectedFailure


class ReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.lab = Lab()
        self.addCleanup(self.lab.db.close)

    def event(self):
        self.lab.accept('tenant-a', 'request-a', {'sku': 'book'})
        self.lab.relay()
        return self.lab.published[0]

    def test_baseline_loses_notification(self):
        with self.assertRaises(InjectedFailure):
            self.lab.baseline()
        self.assertEqual(self.lab.count('orders'), 1)
        self.assertEqual(len(self.lab.published), 0)

    def test_accept_is_atomic(self):
        with self.assertRaises(InjectedFailure):
            self.lab.accept('a', 'key', {}, fail=True)
        self.assertEqual([self.lab.count(t) for t in ('orders', 'requests', 'outbox')], [0, 0, 0])

    def test_request_retry_returns_original_order(self):
        first = self.lab.accept('a', 'key', {'a': 1, 'b': 2})
        self.assertEqual(self.lab.accept('a', 'key', {'b': 2, 'a': 1}), first)
        self.assertEqual(self.lab.count('outbox'), 1)

    def test_changed_payload_conflicts(self):
        self.lab.accept('a', 'key', {'sku': 1})
        with self.assertRaises(ValueError):
            self.lab.accept('a', 'key', {'sku': 2})
        self.assertEqual(self.lab.count('orders'), 1)

    def test_request_keys_are_tenant_scoped(self):
        self.assertNotEqual(self.lab.accept('a', 'key', {}), self.lab.accept('b', 'key', {}))
        self.assertEqual(self.lab.count('orders'), 2)

    def test_publish_failure_keeps_event_pending(self):
        self.lab.accept('a', 'key', {})
        with self.assertRaises(InjectedFailure):
            self.lab.relay(fail_before=True)
        self.assertEqual(self.lab.db.execute('SELECT sent FROM outbox').fetchone()[0], 0)
        self.lab.relay()
        self.assertEqual(len(self.lab.published), 1)

    def test_relay_crash_duplicates_delivery_but_not_effect(self):
        self.lab.accept('a', 'key', {})
        with self.assertRaises(InjectedFailure):
            self.lab.relay(fail_after=True)
        self.lab.relay()
        self.assertEqual(len(self.lab.published), 2)
        self.assertEqual(self.lab.published[0], self.lab.published[1])
        self.assertEqual([self.lab.consume(e) for e in self.lab.published], ['applied', 'duplicate'])
        self.assertEqual(self.lab.effects(), 1)

    def test_consumer_transaction_rolls_back_receipt_and_effect(self):
        event = self.event()
        with self.assertRaises(InjectedFailure):
            self.lab.consume(event, fail=True)
        self.assertEqual((self.lab.count('receipts'), self.lab.effects()), (0, 0))
        self.lab.consume(event)
        self.assertEqual(self.lab.effects(), 1)

    def test_lost_ack_is_safe_to_retry(self):
        event = self.event()
        self.lab.consume(event)  # Commit succeeds, queue acknowledgement is lost.
        self.assertEqual(self.lab.consume(event), 'duplicate')
        self.assertEqual(self.lab.effects(), 1)

    def test_partial_batch_reports_only_failed_message_id(self):
        self.lab.accept('a', 'one', {})
        self.lab.accept('a', 'two', {})
        self.lab.relay()
        messages = [{'messageId': f'message-{i}', 'body': e} for i, e in enumerate(self.lab.published)]
        self.assertEqual(self.lab.batch(messages, {'message-1'}),
                         {'batchItemFailures': [{'itemIdentifier': 'message-1'}]})
        self.assertEqual(self.lab.effects(), 1)
        self.assertEqual(self.lab.batch([messages[1]]), {'batchItemFailures': []})
        self.assertEqual(self.lab.effects(), 2)


if __name__ == '__main__':
    unittest.main()
