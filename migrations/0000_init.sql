CREATE TABLE queue (
  id varchar(128) PRIMARY KEY,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,

  scheduled_for TIMESTAMP NOT NULL,
  failed_attempts INT NOT NULL,
  status INT NOT NULL,
  message TEXT NOT NULL
);
CREATE INDEX index_queue_on_scheduled_for ON queue (scheduled_for);
CREATE INDEX index_queue_on_status ON queue (status);
