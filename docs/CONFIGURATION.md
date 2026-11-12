# Configuration

`.env.example` contains local-development defaults. The API and workers need
compatible MongoDB/Redis URLs; Kubernetes/Compose override selected settings
through their manifests. Defaults below are read from source code unless
otherwise noted.

| Variable                    | Default                                                               | Used by                        | Purpose                                        |
| --------------------------- | --------------------------------------------------------------------- | ------------------------------ | ---------------------------------------------- |
| `PORT`                      | `3000`                                                                | API                            | Express API/page listener                      |
| `MONGODB_URI`               | `mongodb://127.0.0.1:27017/actorlab`                                  | API and worker                 | MongoDB connection                             |
| `REDIS_URL`                 | `redis://127.0.0.1:6379`                                              | API and worker                 | Redis connection                               |
| `WORKER_ID`                 | hostname plus process ID                                              | Worker                         | Stable ID for registration                     |
| `WORKER_PORT`               | `3101`                                                                | Worker                         | Worker HTTP listener                           |
| `WORKER_ADVERTISE_HOST`     | `POD_IP`, else derived pod DNS if `POD_NAME` exists, else `127.0.0.1` | Worker                         | Host component of registered URL               |
| `COORDINATOR_URL`           | `http://localhost:3000`                                               | Worker                         | Base API URL used for `/internal` calls        |
| `RUNTIME_MODE`              | `local`                                                               | Worker                         | Informational worker metadata                  |
| `HEARTBEAT_INTERVAL_MS`     | `4000`                                                                | Worker                         | Heartbeat and singleton lease attempt interval |
| `WORKER_TIMEOUT_MS`         | `15000`                                                               | Runtime service                | Worker registry liveness threshold             |
| `ACTOR_IDLE_TIMEOUT_MS`     | `60000` in code; `300000` in demo config                              | Worker                         | Inactivity before an actor becomes idle/deactivates |
| `ACTOR_DEACTIVATION_INTERVAL_MS` | `10000`                                                         | Worker                         | How often a worker checks for idle actors      |
| `WORKER_REQUEST_TIMEOUT_MS` | `5000`                                                                | Runtime service                | Ownership wait, API-to-worker request timeout  |
| `SINGLETON_LEASE_MS`        | `12000`                                                               | Runtime service                | Singleton Redis lease TTL                      |
| `K8S_NAMESPACE`             | `actorlab`                                                            | Kubernetes service             | Namespace for pod and Deployment operations    |
| `WORKER_DEPLOYMENT`         | `actor-worker`                                                        | Kubernetes service             | Deployment name the API reads/scales           |
| `K8S_REQUEST_TIMEOUT_MS`    | `3000`                                                                | Kubernetes service             | Kubernetes request timeout                     |
| `POD_NAME`                  | Not set in local `.env`; populated by worker pod manifest             | Worker                         | Pod name used for service DNS fallback         |
| `POD_IP`                    | Not set in local `.env`; populated by worker pod manifest             | Worker                         | Pod IP advertising fallback                    |
| `NODE_ENV`                  | Not set by application                                                | Docker image sets `production` | Node environment marker                        |

The sample `.env.example` sets `WORKER_ID=worker-local-1`,
`WORKER_ADVERTISE_HOST=127.0.0.1`, and the default values above. Give each
additional local worker a unique ID and port. Its advertised address must be
reachable from the API.

`HEARTBEAT_INTERVAL_MS` is for worker liveness only. It does not reset an
actor's `lastActiveAt` timestamp and does not stop idle deactivation. For a
presentation, the supplied `.env.example` and Kubernetes ConfigMap set the
actor idle timeout to five minutes. An individual actor can also be marked
**Keep warm for demo** from its details page; that bypasses only its idle
deactivation sweep.

## npm scripts

| Command          | Actual script                                                   |
| ---------------- | --------------------------------------------------------------- |
| `npm start`      | `node src/server.js`                                            |
| `npm run dev`    | `node --watch src/server.js`                                    |
| `npm run worker` | `node src/server.js --worker`                                   |
| `npm run format` | Prettier for supported files, followed by EJS formatting helper |

There is no `test` or `build` npm script in the project manifest.
