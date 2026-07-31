# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

# git is needed at RUNTIME here, not only at build time: every run clones its own
# target repository. That is the difference from this platform's other images,
# where git is a build-only dependency and gets removed again.
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# agentic-events resolves over anonymous HTTPS. This build previously mounted a PAT
# as a BuildKit secret because agentic-sdlc-eventbus was private; it is public now,
# so no credential is involved at build time at all.
#
# The RUNTIME PAT is a separate concern and is unchanged: every run clones its own
# target repository, reading the token from GIT_PAT_FILE via a credential helper
# invoked at request time. That is why git is installed above.
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Runs clone into here; also holds the audit JSONL. Mounted as a volume in compose
# so a restart does not orphan a parked run's workspace.
RUN mkdir -p /workspaces

# Empty by design. Replay-mode fixtures describe work on one specific service, so
# this platform ships none - mount your own here to enable replay mode. See
# docs/adr/0001.
RUN mkdir -p /fixtures

CMD ["python", "-m", "agentic_control_plane.main"]
