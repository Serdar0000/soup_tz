CREATE TABLE IF NOT EXISTS projects (
    id          uuid PRIMARY KEY,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id          uuid PRIMARY KEY,
    project_id  uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    key_hash    bytea NOT NULL UNIQUE,
    prefix      text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);

CREATE TABLE IF NOT EXISTS runs (
    id          uuid PRIMARY KEY,
    project_id  uuid NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name        text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    last_seq    bigint NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS runs_project_idx ON runs(project_id, created_at DESC);

CREATE TABLE IF NOT EXISTS metrics (
    run_id      uuid NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    seq         bigint NOT NULL,
    key         text NOT NULL,
    step        bigint NOT NULL,
    value       double precision NOT NULL,
    ts          timestamptz,
    received_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, key, step)
);
