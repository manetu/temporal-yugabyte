-- Migration v1.2: CHASM separate archetype ID space, and namespace ID uniqueness
--
-- Holds the current-execution record for CHASM archetypes other than Workflow, in a separate
-- ID space from current_executions so a workflow and a CHASM entity may share a business ID
-- without colliding on the same current record. Purely additive: existing rows in
-- current_executions are untouched, and no data migration is required.
CREATE TABLE current_chasm_executions (
  shard_id                       int,
  namespace_id                   uuid,
  workflow_id                    text,
  archetype_id                   int,
  current_run_id                 uuid,
  execution_state                blob,
  execution_state_encoding       text,
  workflow_last_write_version    bigint,
  workflow_state                 int,
  PRIMARY KEY  ((shard_id, namespace_id, workflow_id, archetype_id))
) WITH transactions = { 'enabled' : true };

-- The namespaces table's primary key is `name`; `id` is only a secondary index
-- (namespace_by_id_idx), which does not enforce uniqueness in YCQL. This table gives `id` a
-- real, atomically-checked uniqueness constraint by participating in the same distributed
-- transaction as writes to `namespaces`, so creating a namespace whose id already exists under
-- a different name is rejected instead of silently succeeding.
CREATE TABLE namespaces_by_id (
  id                             uuid,
  name                           text,
  PRIMARY KEY (id)
) WITH transactions = { 'enabled' : true };
