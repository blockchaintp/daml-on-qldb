create extension pgcrypto;
create table kv (
  id serial primary key,
  address bytea not null,
  data bytea not null
);

create unique index unique_addr_index on kv(sha512(address));
