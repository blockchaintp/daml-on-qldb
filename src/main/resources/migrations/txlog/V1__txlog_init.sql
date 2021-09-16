create table tx_data (
  id: uuid primary key DEFAULT gen_random_uuid(),
  data: bytea
)
CREATE INDEX tx_data_idx ON kv(id);

create table tx_seq (
  id bytea primary key not null,
  seq serial,
  constraint fk_data foreign key(id) references tx_data(id)
)

CREATE INDEX tx_seq_seq_idx ON tx_seq(seq);
CREATE INDEX tx_seq_id_idx ON tx_seq(id);

create or replace view tx_log_committed as select tx_data.id,tx_data.data,tx_seq.seq from tx_data,tx_seq where tx_data.id = tx_seq.id;
