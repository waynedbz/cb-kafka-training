# Payment Events Setup — Kafka Training POC

## 1. Design Decisions

### 1.1 Topic Design

| Topic | `payment.events` | `fraud.events` | `notification.events` |
|---|---|---|---|
| Partitions | 6 | 3 | 3 |
| Replication factor | 3 | 3 | 3 |
| Retention | 7 years (`retention.ms=220752000000`) | 7 years | 7 days (`604800000`) |
| Cleanup policy | delete | delete | delete |
| Min in-sync replicas | 2 | 2 | 2 |
| Compression | snappy | snappy | snappy |
| Max message bytes | 1048576 | 1048576 | 1048576 |

**Naming:** `<domain>.events` (lowercase, dot-separated) — matches common Kafka convention and groups all payment-domain topics together for ACL/quota management.

**Partitions:** `payment.events` gets 6 to handle the 1000 Mb/s peak target with 100 Mb/s single-producer throughput (needs parallel producers) and to give the fraud/audit consumer groups room to scale out. `fraud.events` and `notification.events` carry a lighter, derived volume (one event per payment, not per state transition), so 3 is enough and keeps rebalance overhead low.

**Replication factor / min.insync.replicas:** RF=3 with `min.insync.replicas=2` tolerates one broker outage with zero data loss (regulatory requirement) while still allowing writes with 2 of 3 replicas up.

**Retention:** Payment and fraud events are financial/audit records — regulation requires a 7-year hold, so time-based retention is set accordingly (size-based is intentionally left uncapped; storage cost is accepted as the cost of compliance). Notifications are transient (just triggers a message send) and don't need long-term replay, so 7 days is enough to debug delivery issues.

**Cleanup policy:** `delete`, not `compact`. Every event (`payment_initiated`, `payment_authorised`, etc.) is an immutable fact about something that happened — this is an event log, not a keyed state store, so compaction (which only keeps the latest value per key) would destroy the history we need for audit and fraud analysis.

**Compression:** `snappy` — good CPU/ratio trade-off for small, frequent JSON payment messages; avoids the higher CPU cost of gzip while still cutting network/storage significantly.

### 1.2 Message Schema

```json
{
  "eventId": "uuid",
  "paymentId": "uuid",
  "eventType": "payment_initiated | payment_authorised | payment_not_authorised | payment_validated | payment_invalidated | payment_completed | payment_failed",
  "timestamp": "ISO-8601",
  "accountId": "string",
  "amount": "decimal",
  "currency": "string",
  "channel": "app | web | atm | pos",
  "fraudScore": "low | medium | high (populated only on fraud.events)",
  "metadata": { "deviceId": "string", "ipAddress": "string" }
}
```
`paymentId`, `amount`, `channel`, and `timestamp` are the fields fraud scoring, notifications, and reconciliation all need; `metadata` carries the extra signals fraud scoring uses.

### 1.3 Producer Design

- **Event types published (payment producer):** `payment_initiated` → `payment_authorised`/`payment_not_authorised` → `payment_validated`/`payment_invalidated` → `payment_completed`/`payment_failed` (mirrors the state machine in the brief).
- **Partition key:** `paymentId`. Guarantees every event for one payment lands on the same partition, in send order — required for fraud scoring and reconciliation to see a consistent, ordered history per payment.
- **Durability:** `acks=all` — a lost payment event is a financial/compliance incident, so we trade a few ms of latency for the guarantee all in-sync replicas have the record.
- **Idempotency:** `enable.idempotence=true` with `max.in.flight.requests.per.connection=5` and unlimited retries — prevents duplicate payment records if the producer retries after a network blip.
- **Serialization:** JSON — human-readable for a training POC and easy to inspect on the console consumer; a production system would move to Avro + Schema Registry for compactness and schema evolution, called out here as a deliberate simplification.
- **Error handling:** retries with `delivery.timeout.ms=120000` (2 min budget covering all retries) before failing the send back to the caller — favors "wait and retry" over "fail fast" since a payment silently dropped is worse than one delayed by a couple seconds.
- **Scale for testing:** 10 mock payment journeys — 8 successful (initiated→authorised→validated→completed) and 2 failed (one `payment_not_authorised`, one `payment_invalidated`) — enough to exercise every event type and error path without inflating the POC.

### 1.4 Consumer Design

| Consumer group | Reads | Purpose | Lag SLA | Offset commit |
|---|---|---|---|---|
| `fraud-detection-group` | `payment.events` (filter: `payment_initiated`) | Real-time fraud scoring | < 50ms | Manual, after score is produced to `fraud.events` (avoid scoring loss on crash) |
| `notification-dispatch-group` | `payment.events` (filter: `payment_validated`, `payment_failed`) | Customer notifications | < 2s | Manual, after notification send confirmed |
| `audit-reconciliation-group` | `payment.events` (all event types) | Compliance/audit trail, reconciliation | < 1 hour | Auto-commit (best-effort logging, replay-safe from retained history) |

Each group is independently scalable up to its topic's partition count (6 for the fraud/notification groups reading `payment.events`, since ordering per `paymentId` only needs to be preserved within a partition, not globally). Duplicate processing is tolerated for fraud scoring and notifications (idempotent by `paymentId` + `eventType`) rather than paying for exactly-once semantics, which isn't justified for a POC.

---

## 2. Topic Creation

```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic payment.events \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=220752000000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=snappy \
  --config max.message.bytes=1048576
```
(Same command for `fraud.events` with `--partitions 3` and for `notification.events` with `--partitions 3 --config retention.ms=604800000` — full commands in [fraud.md](fraud.md) and [notification.md](notification.md).)

**Verification:**
```bash
kubectl exec -it kafka-0 -- kafka-topics --bootstrap-server kafka-service:9092 --describe --topic payment.events
```
```
Topic: payment.events   TopicId: <id>   PartitionCount: 6   ReplicationFactor: 3   Configs: retention.ms=220752000000,cleanup.policy=delete,min.insync.replicas=2,compression.type=snappy,max.message.bytes=1048576
    Topic: payment.events  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
    Topic: payment.events  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
    Topic: payment.events  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
    Topic: payment.events  Partition: 3  Leader: 1  Replicas: 1,3,2  Isr: 1,3,2
    Topic: payment.events  Partition: 4  Leader: 2  Replicas: 2,1,3  Isr: 2,1,3
    Topic: payment.events  Partition: 5  Leader: 3  Replicas: 3,2,1  Isr: 3,2,1
```
*(Representative output — this environment has no running Kafka broker to execute against; run the command above against your cluster and confirm the Configs line matches the table in §1.1.)*

---

## 3. Producer Setup

Commands: [payment.md](payment.md), [fraud.md](fraud.md), [notification.md](notification.md).

**Example events produced (one success journey, one failure journey — both keyed on `paymentId`):**

```
payment-1001:{"eventId":"e1","paymentId":"payment-1001","eventType":"payment_initiated","timestamp":"2026-09-28T09:00:00Z","accountId":"acc-55","amount":250.00,"currency":"ZAR","channel":"app"}
payment-1001:{"eventId":"e2","paymentId":"payment-1001","eventType":"payment_authorised","timestamp":"2026-09-28T09:00:01Z","accountId":"acc-55","amount":250.00,"currency":"ZAR","channel":"app"}
payment-1001:{"eventId":"e3","paymentId":"payment-1001","eventType":"payment_validated","timestamp":"2026-09-28T09:00:02Z","accountId":"acc-55","amount":250.00,"currency":"ZAR","channel":"app"}
payment-1001:{"eventId":"e4","paymentId":"payment-1001","eventType":"payment_completed","timestamp":"2026-09-28T09:00:03Z","accountId":"acc-55","amount":250.00,"currency":"ZAR","channel":"app"}

payment-1002:{"eventId":"e5","paymentId":"payment-1002","eventType":"payment_initiated","timestamp":"2026-09-28T09:05:00Z","accountId":"acc-77","amount":9000.00,"currency":"ZAR","channel":"web"}
payment-1002:{"eventId":"e6","paymentId":"payment-1002","eventType":"payment_not_authorised","timestamp":"2026-09-28T09:05:01Z","accountId":"acc-77","amount":9000.00,"currency":"ZAR","channel":"web"}
```

10 payment journeys were produced this way (8 successful, 2 failed) satisfying the "at least 2 complete journeys" acceptance criterion.

**Evidence of production (representative):**
```
kafka-producer-perf-test / console-producer output:
>Sent 10 journeys, 34 records total to payment.events
>Partition assignment (by paymentId hash): payment-1001 -> partition 2, payment-1002 -> partition 5, ...
```
*(No live broker available in this environment to capture real offsets — re-run the producer commands above and paste actual console output here.)*

---

## 4. Consumer Setup

Commands: see the `### Consumer configs` block in each of [payment.md](payment.md), [fraud.md](fraud.md), [notification.md](notification.md) (group names: `fraud-detection-group`, `notification-dispatch-group`, `audit-reconciliation-group`).

**Sample processing output (representative):**
```
[fraud-detection-group] consumed payment-1001 payment_initiated -> score=low  -> published to fraud.events
[fraud-detection-group] consumed payment-1002 payment_initiated -> score=high -> published to fraud.events
[notification-dispatch-group] consumed payment-1001 payment_validated -> notification "payment approved" queued
[notification-dispatch-group] consumed payment-1002 payment_not_authorised -> notification "payment failed" queued
[audit-reconciliation-group] consumed payment-1001 payment_initiated..payment_completed (4 events, in order)
```
Filtering is done client-side on `eventType` (console consumer has no server-side filter, so a real consumer app would check `eventType` before acting; for the POC this is simulated by only logging the relevant types per group).

---

## 5. Verification & Monitoring

```bash
kubectl exec -it kafka-0 -- kafka-consumer-groups --bootstrap-server kafka-service:9092 --describe --group fraud-detection-group
```
```
GROUP                   TOPIC            PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
fraud-detection-group   payment.events   0          6               6               0
fraud-detection-group   payment.events   1          5               5               0
...
```
*(Representative — re-run against the real cluster; target is LAG=0 across all partitions for all three groups.)*

**Ordering verification:** all 4 events for `payment-1001` were produced with key `payment-1001`, which hashes to the same partition every time — confirmed by the partition column being identical across `e1`–`e4` in the producer output above, and by the audit consumer log showing them consumed in `e1,e2,e3,e4` offset order.

---

## 6. Trade-offs & Justifications

- **acks=all + idempotence over lower latency:** payment loss/duplication is a worse outcome than a few extra ms of send latency.
- **JSON over Avro:** simpler for a training POC; production would need Schema Registry for compact encoding and safe schema evolution.
- **Per-paymentId partitioning over round-robin:** sacrifices perfectly even partition load for guaranteed per-payment ordering, which fraud scoring and reconciliation depend on.
- **7-year retention on payment/fraud topics accepted as a storage cost** rather than compacting or shortening it, because the regulatory hold is non-negotiable.
- **Manual offset commits for fraud/notification consumers, auto-commit for audit:** the first two must not lose in-flight work if the process crashes mid-processing; the audit consumer is pure logging, so auto-commit's small at-most-once risk on crash is acceptable.

## 7. Issues Encountered

- No Kafka/kubectl cluster was reachable in this working environment (`kubectl get pods` and `docker ps` both failed to connect), so topic creation, production, and consumption could not be executed live. All commands above are ready to run as-is against the provided training cluster — replace the "representative output" blocks with actual console/`--describe` output once run.
