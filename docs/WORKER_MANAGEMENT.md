# Worker management

## Worker startup and registration

A worker is started with `node src/server.js --worker` (`npm run worker`).
It connects to MongoDB and Redis, listens on `WORKER_PORT` (default 3101), and
registers with the coordinator at `COORDINATOR_URL` (default
`http://localhost:3000`). Its URL is built from `WORKER_ADVERTISE_HOST`,
`POD_IP`, or a pod DNS name fallback.

In Kubernetes, `POD_NAME` is the worker ID; namespace and pod IP are taken from
the Downward API. Locally, `WORKER_ID` or a host/process fallback is used.
After startup, a periodic task (default 4 seconds) reads the worker's local
health endpoint and posts its ID, URL, local queue-map size, host, and runtime
mode to `/internal/workers/heartbeat`. The health value is not a count of all
MongoDB actors assigned to the worker. If the coordinator reports that the
registration expired, it re-registers. Worker discovery and heartbeat behavior
are shown in [worker_registration.mmd](diagrams/worker_registration.mmd).

```mermaid
sequenceDiagram
  participant Worker
  participant API as Internal coordinator routes
  participant Redis
  Worker->>Worker: Connect to MongoDB and Redis then start HTTP server
  Worker->>API: Register worker ID and advertised URL
  API->>Redis: Store registration and heartbeat timestamp
  loop Every heartbeat interval
    Worker->>API: Heartbeat with worker metadata
    API->>Redis: Update worker hash
    Worker->>API: Acquire or renew singleton lease
  end
```

The Redis registry stores JSON in hash `actorlab:workers`. `listWorkers`
filters heartbeat timestamps against a 15-second default timeout and deletes
expired or invalid records when called. There is no separate liveness
controller.

## Placement and routing

Actor creation leaves ownership unset. On first message the runtime chooses a
live worker with the fewest actor documents assigned to it. Later messages use the Redis owner map and
MongoDB assignment, repairing ownership if the current owner is no longer
live. The worker validates the assignment generation before executing.

## Actor instances and durable mailbox

Each worker holds a map of per-actor promise tails. Redis execution leases
serialize the same actor across Pods, with ownership-generation checks and
MongoDB optimistic versions as additional stale-write guards. The mailbox is
backed by MongoDB messages with per-actor sequence numbers. Workers poll and
recover pending or expired processing rows after restart. A Redis execution lease
protects the actor across Pods. A drain endpoint blocks new work and waits for
the active execution lease during migration.

## Failure and shutdown

API requests to an unreachable worker return `503` after the message is stored
as pending. Worker polling resumes pending messages. Expired registration is
pruned by the coordinator, and periodic or registration-triggered rebalancing
can move dead-owner actors.

On SIGTERM/SIGINT, worker stops its heartbeat timer, waits for tracked tails,
closes its HTTP server, compare-and-deletes its current singleton lease if
still held, quits Redis, and exits.

Kubernetes can restart a failed pod and adjust worker replica count, but actor
registration, routing, and automatic ownership migration and recovery are application behaviors. See
[Kubernetes integration](KUBERNETES_INTEGRATION.md).
