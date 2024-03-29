create extension pgcrypto;
create table kv (
  id serial primary key,
  address bytea not null,
  data bytea not null
);

create unique index unique_addr_index on kv(sha512(address));

create table tx (
  id uuid primary key,
  seq serial,
  data bytea,
  complete bool default false,
  ts timestamp
);

alter sequence tx_seq_seq minvalue 0 restart with 0;

create index tx_id_idx on tx(id);
create index tx_seq_idx on tx(seq);
