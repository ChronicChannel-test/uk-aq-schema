-- UK AQ v0.2.0 connector-specific checkpoint tables.
--
-- Connector-specific names keep the Communities and future Nodes ingest
-- state independent while both remain part of the Breathe London service.

create schema if not exists uk_aq_raw;
set search_path = uk_aq_raw, uk_aq_core, public;

create table if not exists blondon_communities_timeseries_checkpoints (
  station_id bigint not null references stations(id) on delete cascade,
  species text not null,
  timeseries_id bigint references timeseries(id) on delete set null,
  last_observed_at timestamptz,
  last_polled_at timestamptz,
  last_error text,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  primary key (station_id, species)
);

create index if not exists blondon_communities_timeseries_checkpoints_last_obs_idx
  on blondon_communities_timeseries_checkpoints(last_observed_at);

create table if not exists blondon_communities_station_checkpoints (
  station_id bigint primary key references stations(id) on delete cascade,
  next_due_at timestamptz,
  last_observed_at timestamptz,
  ingest_lag_samples int[] not null default '{}'::int[],
  last_polled_at timestamptz,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists blondon_communities_station_checkpoints_next_due_at_idx
  on blondon_communities_station_checkpoints(next_due_at);
create index if not exists blondon_communities_station_checkpoints_last_polled_at_idx
  on blondon_communities_station_checkpoints(last_polled_at);

create table if not exists blondon_nodes_station_checkpoints (
  station_id bigint primary key references stations(id) on delete cascade,
  next_due_at timestamptz,
  last_observed_at timestamptz,
  ingest_lag_samples integer[] not null default '{}'::integer[],
  last_polled_at timestamptz,
  last_error text,
  species_last_observed_at jsonb not null default '{}'::jsonb,
  species_last_error jsonb not null default '{}'::jsonb,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists blondon_nodes_station_checkpoints_next_due_at_idx
  on blondon_nodes_station_checkpoints(next_due_at);
create index if not exists blondon_nodes_station_checkpoints_last_polled_at_idx
  on blondon_nodes_station_checkpoints(last_polled_at);
