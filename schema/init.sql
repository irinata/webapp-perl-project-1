CREATE TABLE IF NOT EXISTS message (
  created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
  id VARCHAR NOT NULL,
  int_id CHAR(16) NOT NULL,
  str VARCHAR NOT NULL,
  status BOOL,
  CONSTRAINT message_id_pk PRIMARY KEY(id)
);

CREATE INDEX IF NOT EXISTS message_created_idx ON message (created);

CREATE INDEX IF NOT EXISTS message_int_id_idx ON message (int_id);

CREATE TABLE IF NOT EXISTS log (
  created TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
  int_id CHAR(16) NOT NULL,
  str VARCHAR,
  address VARCHAR
);
