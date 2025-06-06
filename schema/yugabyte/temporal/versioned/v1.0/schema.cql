CREATE TYPE serialized_event_batch (
  encoding_type text,
  version       int,
  data          blob
);

CREATE TABLE shards (
  shard_id                       int,
  shard                          blob,
  shard_encoding                 text,
  range_id                       bigint,  -- Increasing sequence identifier for transfer queue, checkpointed into shard info
  PRIMARY KEY (shard_id)
) WITH transactions = { 'enabled' : true };

CREATE TABLE timers (
  shard_id                       int,
  type                           int, -- enum RowType { TimerTask, HistoryTask }
  visibility_ts                  timestamp,
  task_id                        bigint, -- unique identifier for tasks for an execution
  data                           blob,
  encoding                       text,
  PRIMARY KEY  ((shard_id, type), visibility_ts, task_id)
) WITH transactions = { 'enabled' : true };

CREATE TABLE system_tasks (
  shard_id                       int,
  id                             text, -- slug for { TransferTask, ReplicationTask, VisibilityTask }, ClusterID for DLQ
  task_id                        bigint, -- unique identifier for tasks for an execution
  data                           blob,
  encoding                       text,
  PRIMARY KEY  ((shard_id, id), task_id)
) WITH transactions = { 'enabled' : true };

CREATE TABLE current_executions (
  shard_id                       int,
  namespace_id                   uuid,
  workflow_id                    text,
  current_run_id                 uuid,
  execution_state                blob,
  execution_state_encoding       text,
  workflow_last_write_version    bigint,
  workflow_state                 int,
  PRIMARY KEY  ((shard_id, namespace_id, workflow_id))
) WITH transactions = { 'enabled' : true };

CREATE TABLE executions (
  shard_id                       int,
  namespace_id                   uuid,
  workflow_id                    text,
  run_id                         uuid,
  execution                      blob,
  execution_encoding             text,
  execution_state                blob,
  execution_state_encoding       text,
  next_event_id                  bigint,  -- This is needed to make conditional updates on session history
  activity_map                   map<bigint, blob>,
  activity_map_encoding          text,
  timer_map                      map<text, blob>,
  timer_map_encoding             text,
  child_executions_map           map<bigint, blob>,
  child_executions_map_encoding  text,
  request_cancel_map             map<bigint, blob>,
  request_cancel_map_encoding    text,
  signal_map                     map<bigint, blob>,
  signal_map_encoding            text,
  signal_requested               set<uuid>,
  buffered_events_list           list<frozen<serialized_event_batch>>,
  checksum                       blob,
  checksum_encoding              text,
  db_record_version              bigint,
  PRIMARY KEY  ((shard_id, namespace_id, workflow_id, run_id))
) WITH transactions = { 'enabled' : true };

CREATE TABLE history_node (
  tree_id                        uuid, -- run_id if no reset, otherwise run_id of first run
  branch_id                      uuid, -- changes in case of reset workflow. Conflict resolution can also change branch id.
  node_id                        bigint, -- == first eventID in a batch of events
  txn_id                         bigint, -- in case of multiple transactions on same node, we utilize highest transaction ID. Unique.
  prev_txn_id                    bigint, -- point to the previous node: event chaining
  data                           blob, -- batch of workflow execution history events as a blob
  data_encoding                  text, -- protocol used for history serialization
  PRIMARY KEY ((tree_id), branch_id, node_id, txn_id )
) WITH CLUSTERING ORDER BY (branch_id ASC, node_id ASC, txn_id DESC)
  AND transactions = { 'enabled' : true };

CREATE TABLE history_tree (
  tree_id                        uuid,
  branch_id                      uuid,
  branch                         blob,
  branch_encoding                text,
  PRIMARY KEY ((tree_id), branch_id )
) WITH transactions = { 'enabled' : true };

-- Stores activity or workflow tasks
CREATE TABLE tasks (
  namespace_id                   uuid,
  task_queue_name                text,
  task_queue_type                int, -- enum TaskQueueType {ActivityTask, WorkflowTask}
  type                           int, -- enum rowType {Task, TaskQueue}
  task_id                        bigint,  -- unique identifier for tasks, monotonically increasing
  range_id                       bigint, -- Used to ensure that only one process can write to the table
  task                           blob,
  task_encoding                  text,
  task_queue                     blob,
  task_queue_encoding            text,
  PRIMARY KEY ((namespace_id, task_queue_name, task_queue_type), type, task_id)
) WITH transactions = { 'enabled' : true };

-- Stores task queue information such as user provided versioning data
-- OR
-- Used as a mapping from build ID to task queue
CREATE TABLE task_queue_user_data (
  namespace_id                   uuid,
  task_queue_name                text,
  build_id                       text,   -- If this row is used as a mapping of build ID to task queue, this will not be empty
  data                           blob,   -- temporal.server.api.persistence.v1.TaskQueueUserData
  data_encoding                  text,   -- Encoding type used for serialization, in practice this should always be proto3
  version                        bigint, -- Version of this row, used for optimistic concurrency
  -- task_queue_name is not a part of the parititioning key to allow cheaply iterating all task queues in a single
  -- namespace. Access to this table should be infrequent enough that a single partition per namespace can be used.
  -- Note that this imposes a limit on total task queue user data within one namespace (see the relevant single
  -- partition Cassandra limits).
  PRIMARY KEY ((namespace_id), build_id, task_queue_name)
) WITH transactions = { 'enabled' : true };

CREATE TABLE namespaces (
  name                          text,
  id                            uuid,
  detail                        blob,
  detail_encoding               text,
  is_global_namespace           boolean,
  notification_version          bigint,
  PRIMARY KEY (name)
)  WITH transactions = { 'enabled' : true };

CREATE INDEX namespace_by_id_idx on namespaces (id);

CREATE TABLE queue_metadata (
  queue_type                    int,
  cluster_ack_level             map<text, bigint>, -- TODO remove once migration is finished
  data                          blob,
  data_encoding                 text,
  version                       bigint,
PRIMARY KEY (queue_type)
)  WITH transactions = { 'enabled' : true };

CREATE TABLE queue (
  queue_type                    int,
  message_id                    bigint,
  message_payload               blob,
  message_encoding              text,
  PRIMARY KEY  (queue_type, message_id)
) WITH transactions = { 'enabled' : true };

CREATE TABLE cluster_metadata_info (
  metadata_partition           int,
  cluster_name                 text,
  data                         blob,
  data_encoding                text,
  version                      bigint,
  PRIMARY KEY  (metadata_partition, cluster_name)
) WITH transactions = { 'enabled' : true };

CREATE TABLE cluster_membership (
  membership_partition       tinyint,
  host_id                    uuid,
  rpc_address                inet,
  rpc_port                   smallint,
  role                       tinyint,
  session_start              timestamp,
  last_heartbeat             timestamp,
  expiration                 timestamp,
  PRIMARY KEY (membership_partition, role, host_id)
) WITH transactions = { 'enabled' : true };

CREATE INDEX cm_lastheartbeat_idx on cluster_membership (last_heartbeat);
CREATE INDEX cm_sessionstart_idx on cluster_membership (session_start);
CREATE INDEX cm_expiration_idx on cluster_membership (expiration);

CREATE TABLE queues (
  queue_type                 int,
  queue_name                 text,
  metadata_payload           blob,
  metadata_encoding          text,
  version                    bigint,
  PRIMARY KEY ((queue_type, queue_name))
) WITH transactions = { 'enabled' : true };

CREATE TABLE queue_messages (
  queue_type                 int,
  queue_name                 text,
  queue_partition            int,
  message_id                 bigint,
  message_payload            blob,
  message_encoding           text,
  PRIMARY KEY ((queue_type, queue_name, queue_partition), message_id)
) WITH transactions = { 'enabled' : true };

CREATE TABLE nexus_endpoints (
  partition                  int, -- constant for all rows (using a single partition for efficient list queries)
  type                       int, -- enum RowType { PartitionStatus, NexusEndpoint }
  id                         uuid,
  data                       blob,
  data_encoding              text,
  -- When type=PartitionStatus contains the partition version.
  --      Partition version is used to guarantee latest versions when listing all endpoints.
  -- When type=NexusEndpoint contains the endpoint version used for optimistic concurrency.
  version                   bigint,
  PRIMARY KEY ((partition), type, id)
) WITH transactions = { 'enabled' : true };
