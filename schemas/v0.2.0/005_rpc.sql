-- UK AQ v0.2.0 RPC contract.
--
-- Run with psql. The canonical active RPC definitions are maintained in the
-- ingest DB schema file; the Phase 2 driver also reloads the PostgREST schema
-- cache after replacing the public RPC signatures.
\ir ../ingest_db/uk_aq_network_public_contract_phase2.sql
