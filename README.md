# agentwave

agentwave is an embeddable agent assistant for CRM systems, admin panels,
dashboards and internal tools. It turns a short request and the current screen
route into one validated suggestion. The suggestion proposes exactly one action.
The assistant never performs the action. Your application receives the
suggestion and stays in control.

The module has zero external dependencies and contains three packages.

| Package | Purpose |
| --- | --- |
| `agentwave` | Policy engine and bounded job store. |
| `server` | HTTP handler and private Unix socket listener. |
| `codex` | Generation backend that runs the OpenAI Codex CLI (`codex exec`). |

## Use cases

- An admin panel shows an assistant that suggests the next action for the current page.
- A CRM contact list asks for the best next step. The assistant returns a `lookup` suggestion with a search term.
- A dashboard explains its own features when a user asks for help.
- An internal tool routes a question to the correct module.

The application decides what to do with each suggestion. You can show it, log
it, or apply it after your own permission checks.

## Architecture

```
browser or application
        |
        v
your backend
        |
        v
private Unix socket  /run/agentwave/agentwave.sock
        |
        v
Codex CLI  (read-only sandbox, all tools disabled)
        |
        v
model
```

The service listens only on a Unix socket. A process can connect only when it
has access to the socket file. Your backend is the only client. The Codex CLI
runs in a read-only sandbox with a fixed environment and every tool feature
disabled. The model receives the request and route as untrusted data. Every
result must match the policy. A result outside the policy becomes a failed job.

## Quick start

Create a policy file. The example below lists three actions and five modules.

```json
{
  "actions": ["help", "navigate", "lookup"],
  "modules": ["none", "home", "contacts", "deals", "reports"],
  "routePrefix": "/admin",
  "queryActions": {
    "lookup": "contacts"
  }
}
```

Build the binary and run it. The socket path must be absolute.

```sh
go build -o /tmp/agentwave ./cmd/agentwave
/tmp/agentwave \
  --socket /run/agentwave/agentwave.sock \
  --policy /etc/agentwave/policy.json \
  --model gpt-5.6-sol
```

The command `agentwave --version` prints the version and exits. The flags are
`--socket` (required), `--policy` (required), `--model` (required), `--codex`
(default `codex`) and `--home` (optional).

Then call the three routes:

```sh
curl --unix-socket /run/agentwave/agentwave.sock http://localhost/healthz

curl --unix-socket /run/agentwave/agentwave.sock \
  -H 'Content-Type: application/json' \
  -H 'X-Actor-Id: 11111111-1111-4111-8111-111111111111' \
  -d '{"prompt":"Where do I find the contact Acme?","route":"/admin/contacts"}' \
  http://localhost/jobs

curl --unix-socket /run/agentwave/agentwave.sock \
  -H 'X-Actor-Id: 11111111-1111-4111-8111-111111111111' \
  http://localhost/job
```

`POST /jobs` returns a job in the `working` status. Poll `GET /job` until the
status is `ready` or `failed`. See [HTTP API](#http-api) for the status codes.

## Deployment

The binary is a single static file. You can cross-compile it for Linux.

```sh
CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o agentwave ./cmd/agentwave
```

### Docker

Prebuilt images are published for `linux/amd64` and `linux/arm64` when a `v*`
tag is pushed:

```
ghcr.io/m1chlcz/agentwave
m1chl/agentwave
```

The image tag matches the Git tag (for example `v0.1.0`). The `latest` tag
follows stable releases only. Add `--policy`, `--socket` and `--model` like the
local binary.

Build the image yourself and run the container. The policy file is read-only.
The socket lives in a named volume.

```sh
docker build -t agentwave .
docker run --rm \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --tmpfs /tmp \
  -v "$PWD/deploy/policy.json:/etc/agentwave/policy.json:ro" \
  -v agentwave-run:/run/agentwave \
  agentwave
```

The Codex CLI keeps its credentials in `CODEX_HOME` (`/home/agent/.codex`).
With a read-only root file system, mount that directory when the model needs
credentials.

### Compose

`deploy/docker-compose.yml` sets the same limits. Put your policy at
`deploy/policy.json` and start the service:

```sh
docker compose -f deploy/docker-compose.yml up --build
```

### systemd

`deploy/agentwave.service` runs the binary as the `agentwave` user with a strict
file system policy.

```sh
go build -trimpath -ldflags="-s -w" -o agentwave ./cmd/agentwave
install -m 0755 agentwave /usr/local/bin/agentwave
useradd --system --user-group --home-dir /var/lib/agentwave --shell /usr/sbin/nologin agentwave
install -d -m 0755 /etc/agentwave
install -m 0644 cmd/agentwave/policy.example.json /etc/agentwave/policy.json
install -m 0644 deploy/agentwave.service /etc/systemd/system/agentwave.service
systemctl enable --now agentwave
```

## Embedding

A browser cannot open a Unix socket. Your backend proxies the three routes and
adds the actor id. Use one actor id per user or per session. Never accept the
actor id from the browser.

The next example is a small Node.js proxy. It forwards `/assistant/*` to the
socket.

```js
import { createServer, request } from "node:http";

const socketPath = "/run/agentwave/agentwave.sock";
const actorId = "11111111-1111-4111-8111-111111111111";

createServer((incoming, outgoing) => {
  const path = "/" + incoming.url.replace(/^\/assistant\/?/, "");
  const upstream = request(
    {
      socketPath,
      path,
      method: incoming.method,
      headers: {
        "X-Actor-Id": actorId,
        "Content-Type": incoming.headers["content-type"] ?? "application/json",
      },
    },
    (reply) => {
      outgoing.writeHead(reply.statusCode, reply.headers);
      reply.pipe(outgoing);
    },
  );
  upstream.on("error", () => {
    outgoing.writeHead(503).end();
  });
  incoming.pipe(upstream);
}).listen(3000);
```

The browser calls `GET /assistant/healthz`, `POST /assistant/jobs` and
`GET /assistant/job`.

## Policy reference

| Field | Required | Meaning |
| --- | --- | --- |
| `actions` | Yes | Allowed action names. Must contain `help`. |
| `modules` | Yes | Allowed module names. Must contain `none`. |
| `routePrefix` | No | Route scope. The default is `/`. |
| `queryActions` | No | Maps an action to the module that a non-empty query requires. |
| `working` | No | Job message during generation. |
| `failed` | No | Job message after a failed generation. |

The policy fixes the reserved action rules:

- `help` requires an empty query.
- `navigate` requires a module other than `none` and an empty query.
- Each action in `queryActions` requires its configured module and a non-empty query.

The service rejects a suggestion outside the policy. The model cannot invent a
new action or module.

## HTTP API

| Route | Method | Purpose | Responses |
| --- | --- | --- | --- |
| `/healthz` | `GET` | Liveness check. | `200` |
| `/jobs` | `POST` | Accept one prompt and route. | `202`, `401`, `409`, `415`, `422`, `503` |
| `/job` | `GET` | Return the current job of the actor. | `200`, `204`, `401`, `405`, `422` |

Every route except `/healthz` requires exactly one `X-Actor-Id` header with a
lowercase UUID. The body of a `POST /jobs` request is
`{"prompt":"...","route":"..."}` with `Content-Type: application/json`. The
service marks each response as `Cache-Control: no-store`.

## Security model

- The service listens on a private Unix socket with mode `0660`. It opens no TCP port.
- The Codex CLI runs with a read-only sandbox, an empty tool set and a fixed environment. It cannot change files or run commands.
- The policy constrains every suggestion. A result outside the policy becomes a failed job.
- The service has no access to application data, credentials or mutation endpoints. It stores only the prompts, routes and suggestions that callers send.
- The container and the systemd unit run as a non-root user.
- The store keeps at most one job per actor, at most 32 entries for 15 minutes, and at most two concurrent generations with a 120 second timeout.

## Development

```sh
gofmt -l .
go vet ./...
go test -race ./...
go build ./cmd/agentwave
```

## License

MIT. See [LICENSE](LICENSE).
