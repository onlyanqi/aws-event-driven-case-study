# AWS implementation and rollout notes

These notes map the local model to a proposed AWS design. No AWS resources or handlers have been deployed by this repository.

## Data and transaction boundaries

Suggested DynamoDB items (conceptual keys, not a schema migration):

| Item | Partition / sort key example | Contents |
| --- | --- | --- |
| Request | `TENANT#t / REQUEST#client-key` | Canonical payload hash, order ID, response, creation time |
| Order | `TENANT#t / ORDER#id` | Accepted status and order fields |
| Outbox | `OUTBOX#event-id / EVENT` | Stable event ID, tenant, order ID, event type/version, pending status, creation time |
| Consumer receipt | `CONSUMER#fulfillment / EVENT#event-id` | Applied timestamp |
| Projection | `TENANT#t / FULFILLMENT#order-id` | Consumer-owned state |

Intake conditionally puts the request, order, and outbox in one transaction. Put the absence condition on the write itself. On a request-key collision, read the request consistently and compare its fingerprint; return its stored result or a conflict. Handle transaction conflicts separately with bounded retries. Derive tenant identity from verified authorization claims, not the JSON body. The local model accepts a tenant argument only to test key isolation, not authentication.

The consumer conditionally puts an absent receipt and updates its own projection in one transaction. If the receipt exists, verify that fact before treating a canceled transaction as a duplicate; throttling and other cancellation causes must remain retryable failures. The model has one projection consumer and deliberately does not emulate AWS cancellation details.

Keep receipt and request retention longer than their supported replay/retry windows. Deleting receipts removes duplicate protection for old events. Decide and document maximum replay age before introducing TTL; do not expire pending outbox items. The local model never expires these records.

## Relay and reconciliation

Filter the DynamoDB stream mapping for inserted outbox items, so sent-marker updates do not trigger republishing loops. Publish a versioned event with its original event ID, then update the sent marker. Retries and overlapping relay invocations can publish duplicates; this is expected.

Provide a time-bucketed, sharded pending index and a scheduled reconciliation invocation of the relay. Recheck pending state when reading candidates. An eventually consistent index can surface stale candidates; duplicate handling remains necessary. Backoff on publishing failures, alarm on oldest pending age, and archive completed records according to retention requirements. This reconciliation path, index, and scheduler are design work still to implement.

## Messaging configuration

Use a standard SNS topic and one standard SQS queue per independent subscriber. Enable raw message delivery for SQS subscriptions so the handler receives the application event without an SNS JSON envelope. [SNS raw delivery documentation.](https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html)

Illustrative starting settings, subject to AWS integration tests and workload measurements:

| Setting | Initial proposal | Reason |
| --- | --- | --- |
| Consumer timeout | 10 seconds | Bound each processing attempt |
| SQS visibility timeout | 60 seconds | Start at six times the function timeout, with no batching window |
| Batch size / window | 10 / 0 seconds | Small batches, no intentional accumulation delay |
| Event source response type | `ReportBatchItemFailures` | Return failed SQS message IDs only |
| Processing DLQ redrive threshold | `maxReceiveCount: 5` | Allow retries before operator investigation |
| Source / processing DLQ retention | 4 / 14 days | Leave time to investigate; confirm supported replay age |

These values follow the shape of [AWS SQS/Lambda configuration guidance](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html), not a measured capacity plan. If a batching window is added, account for it in visibility timeout. Set event-source maximum concurrency and Lambda reserved concurrency together based on downstream capacity; account for every event source sharing the function.

Distinguish **SNS subscription delivery DLQs** (SNS cannot deliver to SQS) from **SQS processing DLQs** (consumer repeatedly fails). Configure and alarm on both failure paths. A successful SNS publish does not prove that every subscription received or processed the message.

## Edge and permissions

- HTTPS on viewer and origin connections. Disable caching for authenticated API routes; forward required auth and idempotency headers. Configure allowed methods and CORS for the intended frontend.
- API Gateway verifies identity; handlers enforce tenant/resource access. CloudFront is not an authorization boundary.
- Intake can write only its DynamoDB items; relay can read/update outbox state and publish only to the intended topic; consumer can receive/delete from its queue and write its receipt/projection.
- Queue resource policy permits the intended SNS topic using a source-ARN condition. Review encryption key permissions when using customer-managed keys.
- Validate event payloads, avoid sensitive body logging, and scope replay tooling to operators with an audit trail.

## Proposed rollout and recovery

1. Provision an isolated environment and run the failure matrix against AWS. Add alarms for pending age, queue age, DLQ depth, Lambda errors/throttles, and stream iterator age.
2. Deploy the idempotent consumer first. Then enable outbox intake for a small cohort. Avoid two publishers emitting different event IDs for the same business event during migration.
3. Compare accepted orders with projected orders, allowing for processing delay. Inspect outbox and queue age before increasing traffic. Persist correlation IDs across request, event, and processing logs.
4. If downstream processing degrades, pause/drain consumers as appropriate while retaining orders and pending events. Do not revert to direct publish while unprocessed outbox records exist without a reconciliation plan.
5. For a DLQ event, inspect its failure, fix the cause, validate schema and replay age, and replay a small sample with the **original event ID**. Observe receipts and projections before increasing replay rate. Never delete receipts to force a replay of a non-idempotent external effect.

## Honest boundaries

This design trades immediate downstream completion for durable asynchronous intent. It adds storage and operational complexity. It does not solve multi-Region conflicts, business compensation, global ordering, or atomicity with third-party side effects. Real AWS integration, operational recovery, and cost evidence are the next milestones.
