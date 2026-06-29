-- UK AQ website/API v2.0.0 hard cut: Phase 2 public RPC contract.
--
-- This migration is a psql driver because the canonical RPC catalog is kept
-- as one ordered file. Relative inclusion prevents a second copy of function
-- bodies from drifting from schemas/ingest_db/uk_aq_rpc.sql.
\ir ../../../ingest_db/uk_aq_network_public_contract_phase2.sql
