-- UK AQ v0.2.0 additive TEST migration: ingest-run network context.

set search_path = uk_aq_core, public, pg_catalog;

alter table uk_aq_ingest_runs
  add column if not exists network_id bigint,
  add column if not exists network_code text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'uk_aq_core.uk_aq_ingest_runs'::regclass
      and conname = 'uk_aq_ingest_runs_network_id_fkey'
  ) then
    alter table uk_aq_ingest_runs
      add constraint uk_aq_ingest_runs_network_id_fkey
      foreign key (network_id)
      references networks(id)
      on delete set null
      not valid;
  end if;
end
$$;

update uk_aq_ingest_runs r
set
  network_id = n.id,
  network_code = n.network_code
from networks n
where r.connector_code in ('breathelondon', 'blondon_communities', 'blondon_nodes')
  and n.network_code = 'breathelondon'
  and (r.network_id is null or r.network_code is null);

update uk_aq_ingest_runs r
set
  network_id = n.id,
  network_code = n.network_code
from networks n
where r.connector_code = 'openaq'
  and n.network_code = 'openaq'
  and (r.network_id is null or r.network_code is null);

update uk_aq_ingest_runs r
set
  network_id = n.id,
  network_code = n.network_code
from networks n
where r.connector_code = 'sensorcommunity'
  and n.network_code = 'sensorcommunity'
  and (r.network_id is null or r.network_code is null);

-- Historical uk_air_sos runs remain network-null until a run can be tied to a
-- specific SOS network with evidence.

create index if not exists uk_aq_ingest_runs_network_run_end_idx
  on uk_aq_ingest_runs(network_id, run_ended_at desc);
