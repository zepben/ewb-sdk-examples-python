# Asset-level HCM studies

These examples upload multi-result EAS studies from Postgres result tables populated by the hosting-capacity service:

- `public.asset_level_thermal_loading_summary`
- `public.asset_level_voltage_summary`
- `public.cim_to_opendss`

The scripts use `.env` for EWB/EAS connection details and Postgres connectivity:

- `EWB_HOST`
- `EWB_RPC_PORT`
- `EWB_ACCESS_TOKEN`
- `RESULT_DB_HOST`
- `RESULT_DB_PORT`
- `RESULT_DB_USER`
- `RESULT_DB_PASSWORD`
- `RESULT_DB_NAME`
- optional `EWB_CA_FILENAME`, `EWB_TIMEOUT_SECONDS`, `EWB_DEBUG`, `EWB_SKIP_CONNECTION_TEST`

## Thermal performance

```bash
PYTHONPATH=src .venv/bin/python \
  src/zepben/examples/studies/hosting_capacity_examples/asset_level_metrics_hcm/asset_level_thermal_performance_study.py \
  --work-package-id <WORK_PACKAGE_UUID> \
  --env-file .env \
  --dry-run
```

Remove `--dry-run` to upload the study. The thermal example creates layers for:

- maximum loading percent
- hours over normal rating
- hours over emergency rating
- import overload energy
- export overload energy

## Voltage performance

```bash
PYTHONPATH=src .venv/bin/python \
  src/zepben/examples/studies/hosting_capacity_examples/asset_level_metrics_hcm/asset_level_voltage_performance_study.py \
  --work-package-id <WORK_PACKAGE_UUID> \
  --env-file .env \
  --dry-run
```

Remove `--dry-run` to upload the study. The voltage example creates layers for:

- lowest endpoint voltage, converted from per-unit to line-ground volts
- highest endpoint voltage, converted from per-unit to line-ground volts
- endpoint voltage bandwidth percent
- total voltage limit hours

## Asset mapping

The result rows are keyed by `conducting_equipment_mrid`. The examples use `public.cim_to_opendss` to expand simplified result MRIDs back to original CIM `AcLineSegment` and `PowerTransformer` MRIDs where those original assets are present in the fetched EWB feeder model.

If mapped original assets are not present in EWB, the scripts fall back to the result MRID when it exists in the network model. This supports both original-CIM and simplified-network visualisation workflows.

By default the scripts resolve result feeder codes, such as `S336`, against EWB feeder names/MRIDs. If that lookup does not match your environment, pass explicit feeder MRIDs:

```bash
--feeder-mrids S336
```
