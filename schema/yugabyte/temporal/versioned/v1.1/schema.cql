-- Migration v1.1: Add CHASM node support and fair task scheduling
ALTER TABLE executions ADD chasm_node_map map<text, blob>;
ALTER TABLE executions ADD chasm_node_map_encoding text;

-- Add tasks_v2 table for fair task scheduling (stride scheduling)
CREATE TABLE tasks_v2 (
  namespace_id                   uuid,
  task_queue_name                text,
  task_queue_type                int,
  type                           int,
  pass                           bigint,
  task_id                        bigint,
  range_id                       bigint,
  ack_level                      bigint,
  task                           blob,
  task_encoding                  text,
  task_queue                     blob,
  task_queue_encoding            text,
  PRIMARY KEY ((namespace_id, task_queue_name, task_queue_type), type, pass, task_id)
) WITH transactions = { 'enabled' : true };
