create table kv (
  id bytea primary key not null,
  data bytea not null
);

CREATE INDEX kv_idx ON kv(id);
