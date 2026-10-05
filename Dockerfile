FROM golang:1.27.0-alpine3.23@sha256:3747dcba41c8b0db3211fda4db61638b980e17ac5bb3c94460a975a9cfe19395 AS build

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/agentwave ./cmd/agentwave

FROM node:26.7.0-alpine3.24@sha256:aadf416b2cdce311a8811ba3f0608a61b77dbf997500e2eafe781b51f6a0b019

RUN npm install --global --ignore-scripts --omit=dev @openai/codex@0.147.0 \
    && npm cache clean --force \
    && adduser -D -H -h /home/agent -s /sbin/nologin agent \
    && mkdir -p /home/agent/.codex /etc/agentwave /run/agentwave \
    && chown -R agent:agent /home/agent /etc/agentwave /run/agentwave

COPY --from=build --chown=agent:agent /out/agentwave /usr/local/bin/agentwave

ENV HOME=/home/agent \
    CODEX_HOME=/home/agent/.codex

USER agent
WORKDIR /tmp

ENTRYPOINT ["/usr/local/bin/agentwave"]
CMD ["--socket","/run/agentwave/agentwave.sock","--policy","/etc/agentwave/policy.json","--model","gpt-5.6-sol"]
