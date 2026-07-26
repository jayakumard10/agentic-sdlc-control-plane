# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# git is needed at RUNTIME here, not only at build time: every run clones its own
# target repository. That is the difference from this platform's other images,
# where git is a build-only dependency and gets removed again.
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# The PAT is mounted as a BuildKit secret and the git config it creates is unset
# within this same layer, so the credential never lands in the built image. Same
# pattern as this platform's other services, which install the same private
# agentic-events dependency.
RUN --mount=type=secret,id=github_pat \
    sh -c ' \
        if [ -f /run/secrets/github_pat ]; then \
            git config --global url."https://$(cat /run/secrets/github_pat)@github.com/".insteadOf "https://github.com/"; \
        fi && \
        pip install --no-cache-dir -r requirements.txt && \
        git config --global --unset url."https://$(cat /run/secrets/github_pat 2>/dev/null)@github.com/".insteadOf 2>/dev/null || true \
    '

COPY . .

# Runs clone into here; also holds the audit JSONL. Mounted as a volume in compose
# so a restart does not orphan a parked run's workspace.
RUN mkdir -p /workspaces

# Empty by design. Replay-mode fixtures describe work on one specific service, so
# this platform ships none - mount your own here to enable replay mode. See
# docs/adr/0001.
RUN mkdir -p /fixtures

CMD ["python", "-m", "agentic_control_plane.main"]
