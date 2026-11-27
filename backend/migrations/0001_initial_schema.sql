-- =====================================================================
-- 0001_initial_schema.sql
-- Distributed Task Queue: authoritative persistent state (PostgreSQL 13+)
-- Owner: database & task lifecycle
-- =====================================================================

-- ---------------------------------------------------------------------
-- Enumerated types
-- ---------------------------------------------------------------------
CREATE TYPE task_status AS ENUM
    ('PENDING', 'ASSIGNED', 'RUNNING', 'COMPLETED', 'RETRY_WAIT', 'FAILED');

CREATE TYPE attempt_status AS ENUM
    ('ASSIGNED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LOST', 'RELEASED');

CREATE TYPE worker_status AS ENUM
    ('ACTIVE', 'SUSPECT', 'DRAINING', 'DEAD', 'STOPPED');

-- ---------------------------------------------------------------------
-- Workers: one row per worker *incarnation*
-- ---------------------------------------------------------------------
CREATE TABLE workers (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    host               TEXT NOT NULL CHECK (length(host) BETWEEN 1 AND 255),
    pid                INTEGER,
    agent_id           TEXT,
    version            TEXT,
    capacity           INTEGER NOT NULL CHECK (capacity BETWEEN 1 AND 32),
    status             worker_status NOT NULL DEFAULT 'ACTIVE',
    labels             JSONB NOT NULL DEFAULT '{}'::jsonb,
    registered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_assigned_at   TIMESTAMPTZ,
    status_changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at         TIMESTAMPTZ,
    stop_reason        TEXT,
    reported_active    INTEGER NOT NULL DEFAULT 0 CHECK (reported_active >= 0),
    tasks_completed    BIGINT NOT NULL DEFAULT 0 CHECK (tasks_completed >= 0),
    tasks_failed       BIGINT NOT NULL DEFAULT 0 CHECK (tasks_failed >= 0),
    CONSTRAINT terminal_workers_have_stop_time
        CHECK (status NOT IN ('DEAD', 'STOPPED') OR stopped_at IS NOT NULL)
);
CREATE INDEX workers_status_idx ON workers (status);
CREATE INDEX workers_name_idx   ON workers (name, registered_at DESC);

-- ---------------------------------------------------------------------
-- Tasks: the persistent queue and each task's current state
-- ---------------------------------------------------------------------
CREATE TABLE tasks (
    id                  UUID PRIMARY KEY,
    seq                 BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,   -- FIFO order
    type                TEXT NOT NULL CHECK (type ~ '^[a-z][a-z0-9_]{1,63}$'),
    payload             JSONB NOT NULL,
    namespace           TEXT NOT NULL DEFAULT 'default'
                        CHECK (namespace ~ '^[a-z][a-z0-9_-]{0,31}$'),
    batch_id            UUID,
    priority            SMALLINT NOT NULL DEFAULT 0 CHECK (priority BETWEEN -10 AND 10),
    status              task_status NOT NULL DEFAULT 'PENDING',
    max_attempts        INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    attempt_count       INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),  -- = fencing token
    timeout_seconds     INTEGER NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 3600),
    idempotency_key     TEXT UNIQUE CHECK (length(idempotency_key) BETWEEN 1 AND 200),
    request_hash        TEXT NOT NULL,
    current_attempt_id  UUID,
    assigned_worker_id  UUID REFERENCES workers (id),
    lease_expires_at    TIMESTAMPTZ,
    available_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    result              JSONB,
    last_error          JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_assigned_at   TIMESTAMPTZ,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,

    CONSTRAINT attempts_within_limit
        CHECK (attempt_count <= max_attempts),
    CONSTRAINT owned_states_have_owner
        CHECK (status NOT IN ('ASSIGNED', 'RUNNING')
               OR (current_attempt_id IS NOT NULL
                   AND assigned_worker_id IS NOT NULL
                   AND lease_expires_at IS NOT NULL)),
    CONSTRAINT unowned_states_have_no_lease
        CHECK (status IN ('ASSIGNED', 'RUNNING') OR lease_expires_at IS NULL),
    CONSTRAINT completed_has_result
        CHECK (status <> 'COMPLETED' OR (result IS NOT NULL AND completed_at IS NOT NULL)),
    CONSTRAINT result_only_when_completed
        CHECK (result IS NULL OR status = 'COMPLETED'),
    CONSTRAINT failed_has_error
        CHECK (status <> 'FAILED' OR (failed_at IS NOT NULL AND last_error IS NOT NULL)),
    CONSTRAINT running_has_start
        CHECK (status <> 'RUNNING' OR started_at IS NOT NULL),
    CONSTRAINT pending_never_attempted
        CHECK (status <> 'PENDING' OR attempt_count = 0)
);

-- Dispatch: eligible tasks in FIFO order (partial index keeps it small)
CREATE INDEX tasks_dispatch_idx     ON tasks (priority DESC, seq)
    WHERE status IN ('PENDING', 'RETRY_WAIT');
-- Recovery: leases that may expire
CREATE INDEX tasks_lease_idx        ON tasks (lease_expires_at)
    WHERE status IN ('ASSIGNED', 'RUNNING');
-- Current ownership per worker (capacity accounting)
CREATE INDEX tasks_owner_active_idx ON tasks (assigned_worker_id)
    WHERE status IN ('ASSIGNED', 'RUNNING');
CREATE INDEX tasks_status_idx       ON tasks (status);
CREATE INDEX tasks_created_idx      ON tasks (created_at DESC);
CREATE INDEX tasks_completed_idx    ON tasks (completed_at) WHERE completed_at IS NOT NULL;
CREATE INDEX tasks_batch_idx        ON tasks (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX tasks_namespace_idx    ON tasks (namespace, created_at DESC);
CREATE INDEX tasks_type_idx         ON tasks (type);

-- ---------------------------------------------------------------------
-- Attempts: complete history of every assignment
-- ---------------------------------------------------------------------
CREATE TABLE task_attempts (
    id               UUID PRIMARY KEY,
    task_id          UUID NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    attempt_no       INTEGER NOT NULL CHECK (attempt_no >= 1),
    worker_id        UUID NOT NULL REFERENCES workers (id),
    status           attempt_status NOT NULL DEFAULT 'ASSIGNED',
    assigned_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    last_renewed_at  TIMESTAMPTZ,
    progress         REAL CHECK (progress IS NULL OR progress BETWEEN 0 AND 1),
    progress_message TEXT,
    duration_ms      INTEGER CHECK (duration_ms IS NULL OR duration_ms >= 0),
    error_type       TEXT,
    error_message    TEXT,
    CONSTRAINT attempt_unique_per_task UNIQUE (task_id, attempt_no),
    CONSTRAINT finished_attempts_have_end
        CHECK (status IN ('ASSIGNED', 'RUNNING') OR finished_at IS NOT NULL)
);
CREATE INDEX attempts_task_idx   ON task_attempts (task_id, attempt_no);
CREATE INDEX attempts_worker_idx ON task_attempts (worker_id, assigned_at DESC);
CREATE INDEX attempts_status_idx ON task_attempts (status);

-- ---------------------------------------------------------------------
-- Lifecycle state machine as data (proposal slide 7)
-- ---------------------------------------------------------------------
CREATE TABLE task_state_transitions (
    from_status task_status NOT NULL,
    to_status   task_status NOT NULL,
    description TEXT NOT NULL,
    PRIMARY KEY (from_status, to_status)
);
INSERT INTO task_state_transitions (from_status, to_status, description) VALUES
    ('PENDING',    'ASSIGNED',   'Broker dispatches the task to a worker with free capacity'),
    ('ASSIGNED',   'RUNNING',    'Worker acknowledged the assignment and started execution'),
    ('ASSIGNED',   'RETRY_WAIT', 'Assignment not acknowledged in time, worker died, or work released'),
    ('RUNNING',    'COMPLETED',  'Current attempt committed a successful result'),
    ('RUNNING',    'RETRY_WAIT', 'Lease expired, worker crashed, timeout, or retryable error'),
    ('RETRY_WAIT', 'ASSIGNED',   'Backoff elapsed; task dispatched again'),
    ('RETRY_WAIT', 'FAILED',     'Retry limit exhausted or non-retryable error');

-- Audit trail written by trigger (cannot be bypassed by application code)
CREATE TABLE task_transitions (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id     UUID NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    from_status task_status,
    to_status   task_status NOT NULL,
    attempt_no  INTEGER NOT NULL,
    worker_id   UUID,
    reason      TEXT,
    at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX task_transitions_task_idx ON task_transitions (task_id, id);

CREATE FUNCTION tasks_enforce_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'PENDING' THEN
            RAISE EXCEPTION 'tasks must be created in PENDING (got %)', NEW.status
                USING ERRCODE = 'DT001';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status THEN
        IF NOT EXISTS (SELECT 1 FROM task_state_transitions
                        WHERE from_status = OLD.status AND to_status = NEW.status) THEN
            RAISE EXCEPTION 'illegal task state transition % -> % (task %)',
                OLD.status, NEW.status, OLD.id
                USING ERRCODE = 'DT001';
        END IF;
    END IF;
    IF NEW.attempt_count < OLD.attempt_count THEN
        RAISE EXCEPTION 'fencing token (attempt_count) must never decrease (task %)', OLD.id
            USING ERRCODE = 'DT002';
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER tasks_enforce_transition
    BEFORE INSERT OR UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_enforce_transition();

CREATE FUNCTION tasks_log_transition() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO task_transitions (task_id, from_status, to_status, attempt_no, worker_id, reason)
        VALUES (NEW.id, NULL, NEW.status, NEW.attempt_count, NULL,
                coalesce(nullif(current_setting('dtq.reason', true), ''), 'submitted'));
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO task_transitions (task_id, from_status, to_status, attempt_no, worker_id, reason)
        VALUES (NEW.id, OLD.status, NEW.status, NEW.attempt_count, NEW.assigned_worker_id,
                nullif(current_setting('dtq.reason', true), ''));
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER tasks_log_transition
    AFTER INSERT OR UPDATE ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_log_transition();

-- Wake the broker when new work is committed (delivered only on COMMIT)
CREATE FUNCTION tasks_notify_new() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('dtq_tasks', 'new');
    RETURN NULL;
END;
$$;

CREATE TRIGGER tasks_notify_new
    AFTER INSERT ON tasks
    FOR EACH STATEMENT EXECUTE FUNCTION tasks_notify_new();

-- ---------------------------------------------------------------------
-- System events (observability log)
-- ---------------------------------------------------------------------
CREATE TABLE system_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    severity    TEXT NOT NULL CHECK (severity IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')),
    component   TEXT NOT NULL CHECK (length(component) BETWEEN 1 AND 40),
    event_type  TEXT NOT NULL CHECK (length(event_type) BETWEEN 1 AND 80),
    message     TEXT NOT NULL,
    task_id     UUID,
    worker_id   UUID,
    namespace   TEXT,
    details     JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX events_ts_idx        ON system_events (ts DESC);
CREATE INDEX events_task_idx      ON system_events (task_id) WHERE task_id IS NOT NULL;
CREATE INDEX events_worker_idx    ON system_events (worker_id) WHERE worker_id IS NOT NULL;
CREATE INDEX events_component_idx ON system_events (component, id DESC);
CREATE INDEX events_type_idx      ON system_events (event_type, id DESC);

CREATE FUNCTION system_events_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    max_id BIGINT;
BEGIN
    SELECT max(id) INTO max_id FROM new_rows;
    IF max_id IS NOT NULL THEN
        PERFORM pg_notify('dtq_events', max_id::text);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER system_events_notify
    AFTER INSERT ON system_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION system_events_notify();

-- ---------------------------------------------------------------------
-- Autoscaling: configuration (intent), controller state, decisions
-- ---------------------------------------------------------------------
CREATE TABLE autoscaler_config (
    id                          SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    mode                        TEXT NOT NULL DEFAULT 'auto' CHECK (mode IN ('auto', 'manual')),
    min_workers                 INTEGER NOT NULL DEFAULT 3,
    max_workers                 INTEGER NOT NULL DEFAULT 8,
    manual_target               INTEGER NOT NULL DEFAULT 3,
    worker_capacity             INTEGER NOT NULL DEFAULT 2,
    scale_up_backlog_per_worker REAL NOT NULL DEFAULT 2.0,
    scale_up_utilization        REAL NOT NULL DEFAULT 0.75,
    scale_down_utilization      REAL NOT NULL DEFAULT 0.25,
    scale_down_backlog          INTEGER NOT NULL DEFAULT 0,
    scale_up_sustain_seconds    INTEGER NOT NULL DEFAULT 6,
    scale_down_sustain_seconds  INTEGER NOT NULL DEFAULT 20,
    cooldown_seconds            INTEGER NOT NULL DEFAULT 15,
    interval_seconds            REAL NOT NULL DEFAULT 2.0,
    scale_step                  INTEGER NOT NULL DEFAULT 1,
    graceful_timeout_seconds    INTEGER NOT NULL DEFAULT 30,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by                  TEXT NOT NULL DEFAULT 'migration',
    CONSTRAINT worker_bounds      CHECK (min_workers >= 0 AND min_workers <= max_workers AND max_workers BETWEEN 1 AND 16),
    CONSTRAINT manual_target_range CHECK (manual_target BETWEEN 0 AND 16),
    CONSTRAINT capacity_range     CHECK (worker_capacity BETWEEN 1 AND 8),
    CONSTRAINT utilization_range  CHECK (scale_up_utilization BETWEEN 0 AND 1
                                         AND scale_down_utilization BETWEEN 0 AND 1
                                         AND scale_down_utilization < scale_up_utilization),
    CONSTRAINT backlog_range      CHECK (scale_up_backlog_per_worker > 0 AND scale_down_backlog >= 0),
    CONSTRAINT sustain_range      CHECK (scale_up_sustain_seconds BETWEEN 0 AND 600
                                         AND scale_down_sustain_seconds BETWEEN 0 AND 3600),
    CONSTRAINT cooldown_range     CHECK (cooldown_seconds BETWEEN 0 AND 3600),
    CONSTRAINT interval_range     CHECK (interval_seconds BETWEEN 0.5 AND 60),
    CONSTRAINT step_range         CHECK (scale_step BETWEEN 1 AND 4),
    CONSTRAINT graceful_range     CHECK (graceful_timeout_seconds BETWEEN 1 AND 600)
);
INSERT INTO autoscaler_config (id) VALUES (1);

CREATE TABLE autoscaler_state (
    id               SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    desired_workers  INTEGER NOT NULL DEFAULT 0,
    actual_workers   INTEGER NOT NULL DEFAULT 0,
    leader_id        TEXT,
    heartbeat_at     TIMESTAMPTZ,
    pressure_since   TIMESTAMPTZ,
    idle_since       TIMESTAMPTZ,
    last_scale_at    TIMESTAMPTZ,
    last_decision    TEXT,
    last_metrics     JSONB NOT NULL DEFAULT '{}'::jsonb
);
INSERT INTO autoscaler_state (id) VALUES (1);

CREATE TABLE scaling_decisions (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    mode          TEXT NOT NULL,
    action        TEXT NOT NULL CHECK (action IN ('scale_up', 'scale_down', 'replace', 'reconcile',
                                                  'hold', 'config_change', 'mode_change', 'manual_target',
                                                  'drain_worker')),
    from_workers  INTEGER NOT NULL,
    to_workers    INTEGER NOT NULL,
    reason        TEXT NOT NULL,
    metrics       JSONB NOT NULL DEFAULT '{}'::jsonb,
    outcome       TEXT NOT NULL DEFAULT 'executed'
                  CHECK (outcome IN ('executed', 'partial', 'failed', 'suppressed', 'recorded')),
    error         TEXT
);
CREATE INDEX scaling_decisions_ts_idx ON scaling_decisions (ts DESC);

-- Operator commands consumed by the autoscaler (single owner of capacity)
CREATE TABLE control_commands (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    command       TEXT NOT NULL CHECK (command IN ('drain_worker')),
    args          JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_by  TEXT NOT NULL DEFAULT 'api',
    status        TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'done', 'failed')),
    result        JSONB,
    processed_at  TIMESTAMPTZ
);
CREATE INDEX control_commands_pending_idx ON control_commands (id) WHERE status = 'pending';

-- Time series sampled by the autoscaler for analytics
CREATE TABLE metrics_samples (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    queue_depth      INTEGER NOT NULL,
    retry_wait       INTEGER NOT NULL,
    assigned         INTEGER NOT NULL,
    running          INTEGER NOT NULL,
    submitted_total  BIGINT NOT NULL,
    completed_total  BIGINT NOT NULL,
    failed_total     BIGINT NOT NULL,
    live_workers     INTEGER NOT NULL,
    busy_workers     INTEGER NOT NULL,
    idle_workers     INTEGER NOT NULL,
    total_slots      INTEGER NOT NULL,
    busy_slots       INTEGER NOT NULL,
    utilization      REAL NOT NULL,
    desired_workers  INTEGER NOT NULL,
    managed_workers  INTEGER NOT NULL
);
CREATE INDEX metrics_samples_ts_idx ON metrics_samples (ts DESC);

-- ---------------------------------------------------------------------
-- Experiment and demonstration records
-- ---------------------------------------------------------------------
CREATE TABLE benchmark_runs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    experiment   TEXT NOT NULL,
    label        TEXT NOT NULL,
    trial        INTEGER NOT NULL DEFAULT 1,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL DEFAULT 'completed',
    config       JSONB NOT NULL DEFAULT '{}'::jsonb,
    metrics      JSONB NOT NULL DEFAULT '{}'::jsonb,
    environment  JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX benchmark_runs_exp_idx ON benchmark_runs (experiment, started_at);

CREATE TABLE demo_runs (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scenario     TEXT NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('running', 'passed', 'failed')),
    started_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at  TIMESTAMPTZ,
    steps        JSONB NOT NULL DEFAULT '[]'::jsonb,
    summary      JSONB NOT NULL DEFAULT '{}'::jsonb,
    error        TEXT
);
CREATE INDEX demo_runs_started_idx ON demo_runs (started_at DESC);
