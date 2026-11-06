# Limitations and future scope

This section distinguishes observed implementation behavior from possible
follow-up work. Suggestions below are not implemented features.

## Current limitations

| Area                    | Current behavior / constraint                                                                                                                                                                                                                                                                      |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Delivery                | Messages are persisted in MongoDB before dispatch and workers poll pending rows. If all workers are unavailable, callers receive an error while the message remains pending until a worker returns or the caller retries.                                                                          |
| Delivery guarantee      | The API may time out after a worker committed state; retrying with the same `messageId` can return a cached result, but there is no end-to-end exactly-once guarantee.                                                                                                                             |
| Idempotency             | Actor documents retain only the latest 256 processed IDs/results. Older retries may be applied again if the message log is missing. `Message.messageId` is globally unique, but an existing ID is returned without checking that it belongs to the requested actor.                                |
| Result logging          | Actor state and the processed-message marker commit before the durable message is acknowledged. Recovery detects a previously committed message and acknowledges it without reapplying the state change. These writes are not a MongoDB transaction.                                               |
| Ordering                | A worker serializes requests locally and obtains an actor-scoped Redis lease across Pods. The lease is renewed during processing and MongoDB version/ownership predicates reject stale commits. The database and Redis cannot atomically commit together.                                          |
| Placement               | New actors remain unassigned until first message, then use least actor count among live workers. When Metrics Server provides data, placement uses reported CPU/memory pressure alongside actor counts; otherwise it falls back to actor counts. It does not account for throughput or actor cost. |
| Rebalancing             | A 30-second API timer, worker registration/drain triggers, and manual trigger rebalance actors using resource metrics when available or actor counts otherwise. A Redis lock prevents overlapping runs; singleton actors are excluded.                                                             |
| Failure recovery        | A later actor route repairs ownership after a worker heartbeat expires; periodic/manual rebalancing can also move dead-owner actors. Failed dispatches leave their durable messages pending for worker polling or caller retry.                                                                    |
| Activation              | Workers keep active actor instances in memory and deactivate them after an idle timeout. Instances are process-local and are reconstructed from MongoDB after restart.                                                                                                                             |
| Singleton               | Lease acquisition and renewal update lease state atomically with Lua. The MongoDB actor update checks and persists its fencing generation; Redis and MongoDB cannot commit atomically together. Redis must be available and suitably configured.                                                   |
| Kubernetes client       | Connection/configuration failure is cached by the client factory. Kubernetes operations are limited to one namespace and the configured worker Deployment.                                                                                                                                         |
| Cluster replica display | The console displays actual Pod readiness, node readiness, Deployment desired/ready replicas, and Redis registration separately. CPU/memory metrics are shown only when Kubernetes Metrics Server returns them; count-based balancing is used when metrics are unavailable.                        |
| Security                | Public API and internal worker coordination routes have no authentication. Sample databases are unauthenticated. No TLS/network policy is configured.                                                                                                                                              |
| Availability            | Sample manifests run one MongoDB and one Redis replica with PVCs. Persistent volumes do not provide Redis or MongoDB high availability, replication, or backup.                                                                                                                                    |
| Observability           | Basic console logging, runtime events, worker heartbeat metadata, and dashboard counters only; no metrics/tracing integration.                                                                                                                                                                     |
| Validation              | The built-in Node test runner includes API integration tests for activation, message sequencing/durability/deduplication, and idle reactivation. Broader failure-injection cases remain manual.                                                                                                    |

## Possible future work

- Add authenticated service-to-service and client APIs, TLS, and namespace
  network policies.
- Introduce a durable queue/stream with explicit retry and dead-letter policy.
- Persist inbox/outbox records transactionally or use a datastore-supported
  atomic protocol for state and result records.
- Keep processed-message deduplication durable and define an explicit
  idempotency-key scope.
- Add a distributed mailbox or stronger fencing/locking across concurrent API
  instances and worker processes.
- Add monitored, policy-driven rebalancing with CPU, memory, queue depth, and
  drain time constraints.
- Make singleton metadata atomic with lease ownership and test Redis failover.
- Add automated integration and failure-injection tests.
- Harden Kubernetes storage, probes, resource requests/limits, secrets, and
  multi-replica service design.

## Discrepancies to keep in mind

The dashboard and in-app guide are explanatory presentations. For precise
behavior, source code is authoritative. In particular, "activation" does not
instantiate a long-lived actor object, and expired worker records do not trigger
an autonomous rebalancing loop. See
[Actor lifecycle](ACTOR_LIFECYCLE.md) and
[Worker management](WORKER_MANAGEMENT.md).
