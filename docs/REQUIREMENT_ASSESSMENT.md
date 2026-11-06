# Runtime requirement assessment

Statuses below reflect behavior verified in the current Minikube demonstration.
They do not describe production availability guarantees.

| Requirement               | Status              | Evidence and remaining gaps                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Virtual actors            | Partially satisfied | Integration tests verified first-message activation, durable per-actor sequence numbers, duplicate suppression, idle deactivation, and state restoration on reactivation. Workers poll MongoDB for pending messages. Worker-crash recovery of a pending in-flight message and concurrent execution across different actors are not yet covered by automated tests.                                                                           |
| Dynamic state rebalancing | Partially satisfied | Scaling from two to three workers triggered automatic migration; 100 actor migration records completed and actors were balanced across registered Pods. Deleting an owner Pod followed by a message recovered the actor state on a replacement Pod. Metrics Server was unavailable, so the resource-aware path was not exercised; it fell back to actor counts. Interrupted-migration and message-during-migration tests remain outstanding. |
| Singleton actors          | Partially satisfied | Acquisition and renewal use atomic Redis scripts with monotonically increasing fencing generations; the live lease was observed during Minikube checks. Stale-token rejection at the protected MongoDB update and failover after a worker restart still need an automated integration test. One Redis instance is not highly available.                                                                                                      |
| Distributed state store   | Partially satisfied | MongoDB persists actor state, message records, sequences, and migration history. Redis stores worker leases and live ownership. MongoDB and Redis now use PVCs in Kubernetes. The prototype remains single-replica, and Redis failover, database outage recovery, and storage restoration have not been fully tested.                                                                                                                        |

## Automated checks

`npm run test:integration` tests actor activation, durable message completion,
sequence uniqueness, duplicate suppression, and idle deactivation/reactivation.
Set `ACTORLAB_API_URL` to a running ActorLab API. The idle lifecycle test waits
for the configured `ACTOR_IDLE_TIMEOUT_MS` (60 seconds by default), plus the
deactivation polling interval. Its wait is configurable with
`ACTOR_IDLE_TEST_WAIT_MS` when running against a deployment with a different
timeout.
