# Hosting Capacity DB-backed studies

This folder contains two study examples that read Hosting Capacity results from
`public.network_performance_metrics_enhanced` in PostgreSQL, map each
`conducting_equipment_mrid` as a measurement-zone head, and propagate metrics
downstream until the next measurement-zone head.

`mz_type` is used to control propagation mode:
- `FEEDER_HEAD`, `FUSE`, `SWITCH`, and `LINE` types are treated as downstream
  zone heads and traced until the next measurement-zone head.
- `TRANSFORMER` is normally treated as a point source.
- Fallback: if a slice only contains `TRANSFORMER`/`FEEDER_HEAD` zones and has
  no explicit LV `FUSE`/`SWITCH`/`LINE` heads, transformers are traced
  downstream so LV line segments are also colour-coded.

## Scripts

- `hcm_asset_metrics_study.py`
  - Uploads six colour-coded downstream **line** metric results:
    - `peak_import`
    - `peak_export`
    - `import_utilisation`
    - `export_utilisation`
    - `load_exceeding_normal_thermal_voltage_kwh`
    - `gen_exceeding_normal_thermal_voltage_kwh`
  - Utilisation is based on measurement-zone head ratings:
    - Transformer heads use transformer end rating (`kVA`).
    - Line-style zones (`FUSE`/`SWITCH`/`LINE`) use the first downstream `AcLineSegment`
      from the zone head and its current rating (`A`).
- `hcm_voltage_heatmap_study.py`
  - Uploads two downstream **line voltage map** results:
    - `v1_avg_section_voltage`
    - `v99_avg_section_voltage`
  - Converts both to line-ground voltage magnitude:
    - `v*_lg = v*_avg_section_voltage * (v_base / sqrt(3))`
  - Applies colour across downstream line segments between measurement-zone heads.

## Required env keys

Read from `.env` by default:

- `EWB_HOST`
- `EWB_RPC_PORT`
- `EWB_ACCESS_TOKEN`
- `INPUT_DB_HOST`
- `INPUT_DB_PORT`
- `INPUT_DB_USER`
- `INPUT_DB_PASSWORD`
- `INPUT_DB_NAME`

## Selection workflow

Both scripts now support a discovery-first interface:

1. Pass `--work-package-id` and list options.
2. Choose values for year/scope/scenario/timestamp.
3. Re-run with selected values, or use interactive prompts.

## Example commands

List selectable options for a work package:

```bash
python hcm_asset_metrics_study.py \
  --work-package-id <wp_id> \
  --list-options
```

Prompt for missing year/scenario/timestamp:

```bash
python hcm_asset_metrics_study.py \
  --work-package-id <wp_id> \
  --zones CPM \
  --interactive
```

From this folder:

```bash
python hcm_asset_metrics_study.py \
  --work-package-id <wp_id> \
  --year 2025 \
  --zones CPM
```

```bash
python hcm_asset_metrics_study.py \
  --work-package-id <wp_id> \
  --year 2025 \
  --feeder-prefixes 271
```

```bash
python hcm_voltage_heatmap_study.py \
  --work-package-id <wp_id> \
  --year 2025 \
  --zones CPM \
  --scenario base \
  --time-of-day all \
  --season all
```

List options for voltage script:

```bash
python hcm_voltage_heatmap_study.py \
  --work-package-id <wp_id> \
  --list-options
```

Use `--dry-run` to build layers without upload.
