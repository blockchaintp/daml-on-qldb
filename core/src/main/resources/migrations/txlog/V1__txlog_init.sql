create extension if not exists "uuid-ossp";

create table tx (
  id uuid primary key,
  seq serial,
  data bytea,
  complete bool default false,
  ts timestamp
);

alter sequence tx_seq_seq minvalue 0 restart with 0;

create or replace function update_changetimestamp_column()
returns trigger as $$
begin
   new.ts = now();
   return new;
end;
$$ language 'plpgsql';

create trigger update_tx_changetimestamp before update
on tx for each row execute procedure
update_changetimestamp_column();


create index tx_id_idx on tx(id);
create index tx_seq_idx on tx(seq);
