-- Remove legacy backup/* manifest references after hard-cut to history/v1.
begin;

do $$
begin
  if to_regclass('uk_aq_ops.prune_day_gates') is not null then
    update uk_aq_ops.prune_day_gates
    set
      history_done = false,
      history_run_id = null,
      history_manifest_key = null,
      history_row_count = null,
      history_file_count = null,
      history_total_bytes = null,
      history_completed_at = null
    where history_manifest_key like 'backup/%';
  end if;

  if to_regclass('uk_aq_ops.history_candidates') is not null then
    delete from uk_aq_ops.history_candidates
    where manifest_key like 'backup/%';
  end if;
end
$$;

commit;
