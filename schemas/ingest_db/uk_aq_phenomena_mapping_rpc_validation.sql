-- Transactional validation for uk_aq_rpc_phenomena_upsert.
-- Requires the Phase 1 mapping table/seeds and Phase 2 RPC. Always rolls back.

begin;

do $$
declare
  v_connector_id integer;
  v_result record;
  v_rejected boolean;
begin
  select id
  into strict v_connector_id
  from uk_aq_core.connectors
  where connector_code = 'blondon_nodes';

  select *
  into strict v_result
  from uk_aq_public.uk_aq_rpc_phenomena_upsert(
    jsonb_build_array(
      jsonb_build_object(
        'connector_id', v_connector_id,
        'source_label', 'breathelondon_nodes:pm2.5',
        'label', 'PM2.5',
        'notation', 'PM2.5',
        'pollutant_label', 'pm2.5',
        'source_uom', 'ug.m-3'
      )
    )
  );

  if v_result.observed_property_code <> 'pm25'
     or v_result.mapping_kind <> 'raw_observed_property'
     or v_result.is_aqi_eligible is not true then
    raise exception 'raw PM2.5 mapping validation failed: %', row_to_json(v_result);
  end if;

  select *
  into strict v_result
  from uk_aq_public.uk_aq_rpc_phenomena_upsert(
    jsonb_build_array(
      jsonb_build_object(
        'connector_id', v_connector_id,
        'source_label', 'breathelondon_nodes:pm2.5:daqi',
        'label', 'PM2.5 DAQI',
        'notation', 'PM2.5 DAQI',
        'pollutant_label', 'daqi_pm25',
        'source_uom', 'DAQI'
      )
    )
  );

  if v_result.observed_property_code is not null
     or v_result.mapping_kind <> 'derived_index'
     or v_result.is_aqi_eligible is not false then
    raise exception 'derived PM2.5 index validation failed: %', row_to_json(v_result);
  end if;

  v_rejected := false;
  begin
    perform uk_aq_public.uk_aq_rpc_phenomena_upsert(
      jsonb_build_array(
        jsonb_build_object(
          'connector_id', v_connector_id,
          'source_label', 'phase2_validation:invalid_daqi_raw',
          'label', 'Invalid DAQI raw',
          'pollutant_label', 'daqi_pm25',
          'source_uom', 'DAQI',
          'mapping_kind', 'raw_observed_property',
          'observed_property_code', 'pm25',
          'is_aqi_eligible', true
        )
      ),
      true
    );
  exception when others then
    v_rejected := true;
  end;
  if not v_rejected then
    raise exception 'DAQI-as-raw input was not rejected';
  end if;

  v_rejected := false;
  begin
    perform uk_aq_public.uk_aq_rpc_phenomena_upsert(
      jsonb_build_array(
        jsonb_build_object(
          'connector_id', v_connector_id,
          'source_label', 'breathelondon_nodes:pm2.5',
          'mapping_kind', 'derived_index'
        )
      )
    );
  exception when others then
    v_rejected := true;
  end;
  if not v_rejected then
    raise exception 'authoritative mapping conflict was not rejected';
  end if;

  select *
  into strict v_result
  from uk_aq_public.uk_aq_rpc_phenomena_upsert(
    jsonb_build_array(
      jsonb_build_object(
        'connector_id', v_connector_id,
        'source_label', 'phase2_validation:admin_no2',
        'label', 'Administrative NO2 validation',
        'notation', 'NO2',
        'pollutant_label', 'no2',
        'source_uom', 'ug/m3',
        'mapping_kind', 'raw_observed_property',
        'observed_property_code', 'no2',
        'is_aqi_eligible', true,
        'mapping_notes', 'Transactional Phase 2 validation.'
      )
    ),
    true
  );
  if v_result.mapping_status <> 'created'
     or v_result.observed_property_code <> 'no2' then
    raise exception 'administrative mapping creation failed: %', row_to_json(v_result);
  end if;

  select *
  into strict v_result
  from uk_aq_public.uk_aq_rpc_phenomena_upsert(
    jsonb_build_array(
      jsonb_build_object(
        'connector_id', v_connector_id,
        'source_label', 'phase2_validation:admin_no2',
        'label', 'Administrative NO2 validation',
        'notation', 'NO2',
        'pollutant_label', 'no2',
        'source_uom', 'ug/m3',
        'mapping_kind', 'raw_observed_property',
        'observed_property_code', 'no2',
        'is_aqi_eligible', true,
        'mapping_notes', 'Transactional Phase 2 validation.'
      )
    ),
    true
  );
  if v_result.mapping_status <> 'existing' then
    raise exception 'administrative mapping idempotency failed: %', row_to_json(v_result);
  end if;

  v_rejected := false;
  begin
    perform uk_aq_public.uk_aq_rpc_phenomena_upsert(
      jsonb_build_array(
        jsonb_build_object(
          'connector_id', v_connector_id,
          'source_label', 'phase2_validation:unknown_code',
          'mapping_kind', 'raw_observed_property',
          'observed_property_code', 'not_a_canonical_code'
        )
      ),
      true
    );
  exception when others then
    v_rejected := true;
  end;
  if not v_rejected then
    raise exception 'unknown canonical code was not rejected';
  end if;

  v_rejected := false;
  begin
    perform uk_aq_public.uk_aq_rpc_phenomena_upsert(
      jsonb_build_array(
        jsonb_build_object(
          'connector_id', v_connector_id,
          'source_label', 'phase2_validation:duplicate'
        ),
        jsonb_build_object(
          'connector_id', v_connector_id,
          'source_label', 'phase2_validation:duplicate'
        )
      )
    );
  exception when others then
    v_rejected := true;
  end;
  if not v_rejected then
    raise exception 'duplicate request key was not rejected';
  end if;

  select *
  into strict v_result
  from uk_aq_public.uk_aq_rpc_phenomena_upsert(
    jsonb_build_array(
      jsonb_build_object(
        'connector_id', v_connector_id,
        'source_label', 'phase2_validation:unknown',
        'label', 'Unknown validation property'
      )
    )
  );

  if v_result.mapping_kind <> 'unknown'
     or v_result.mapping_status <> 'created_unknown'
     or v_result.mapping_warning <> 'unknown_source_label' then
    raise exception 'unknown mapping diagnostic validation failed: %', row_to_json(v_result);
  end if;
end
$$;

rollback;
