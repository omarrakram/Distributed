-- =====================================================================
-- 0002_analytics_views.sql
-- Derived, read-only views used by metrics/analytics endpoints.
-- =====================================================================

-- Per-task timing in milliseconds (queue wait, execution, end-to-end latency)
CREATE VIEW task_timings AS
SELECT t.id,
       t.type,
       t.namespace,
       t.batch_id,
       t.status,
       t.attempt_count,
       t.created_at,
       t.first_assigned_at,
       t.started_at,
       t.completed_at,
       t.failed_at,
       EXTRACT(EPOCH FROM (t.first_assigned_at - t.created_at))::float8 * 1000 AS queue_wait_ms,
       EXTRACT(EPOCH FROM (t.completed_at - t.started_at))::float8 * 1000      AS execution_ms,
       EXTRACT(EPOCH FROM (coalesce(t.completed_at, t.failed_at) - t.created_at))::float8 * 1000 AS latency_ms
  FROM tasks t;

-- Live load per worker incarnation (capacity accounting used by dashboards)
CREATE VIEW worker_load AS
SELECT w.id,
       w.name,
       w.status,
       w.capacity,
       count(t.id) FILTER (WHERE t.status IN ('ASSIGNED', 'RUNNING')) AS active_tasks,
       w.last_heartbeat_at,
       EXTRACT(EPOCH FROM (now() - w.last_heartbeat_at))::float8 AS heartbeat_age_s
  FROM workers w
  LEFT JOIN tasks t ON t.assigned_worker_id = w.id AND t.status IN ('ASSIGNED', 'RUNNING')
 GROUP BY w.id;

-- Recovery delay: for each lost/timed-out attempt, how long until the next attempt started
CREATE VIEW recovery_delays AS
SELECT a.task_id,
       a.attempt_no                 AS failed_attempt_no,
       a.status                     AS failed_status,
       a.error_type,
       a.worker_id                  AS failed_worker_id,
       n.worker_id                  AS next_worker_id,
       coalesce(a.last_renewed_at, a.started_at, a.assigned_at) AS last_sign_of_life_at,
       a.finished_at                AS detected_at,
       n.started_at                 AS next_started_at,
       EXTRACT(EPOCH FROM (a.finished_at - coalesce(a.last_renewed_at, a.started_at, a.assigned_at)))::float8 * 1000
                                    AS detection_ms,
       EXTRACT(EPOCH FROM (n.started_at - coalesce(a.last_renewed_at, a.started_at, a.assigned_at)))::float8 * 1000
                                    AS recovery_ms
  FROM task_attempts a
  JOIN task_attempts n ON n.task_id = a.task_id AND n.attempt_no = a.attempt_no + 1
 WHERE a.status IN ('LOST', 'TIMED_OUT');
