# Studies

This folder contains runnable study scripts that fetch network data, generate GeoJSON overlays, and upload results to EAS.
Most scripts accept a zone code (e.g. `CPM`) and use the shared config at `src/zepben/examples/config.json`.

## Quick start

1. Ensure `src/zepben/examples/config.json` has valid `host`, `access_token`, and `rpc_port`.
2. Run a study from this folder, for example:

```bash
python transformer_utilisation_by_demand.py CPM
```

## Common patterns

- **Zones vs feeders**: Some scripts support explicit feeder MRIDs. For transformer utilisation, use:

```bash
python transformer_utilisation_by_demand.py --mode feeders CPM3B3
```

- **Config override**: Most scripts accept `--config` to point at a different config file.
- **Styles**: Each study uses a companion `style_*.json` file to control map rendering.
- **Outputs**: Studies upload results to EAS and will log progress in the terminal.

## Data quality studies

Data quality scripts live in `data_quality_studies/`. See the dedicated README for usage:

- `src/zepben/examples/studies/data_quality_studies/README.md`

## Troubleshooting

- **Timeouts**: Large zones can take several minutes. Use a longer shell timeout or reduce concurrency if available.
- **404s from Load API**: Some assets may not have demand profiles; the scripts continue and mark those as missing.
- **No features uploaded**: If locations are missing, the study skips upload and logs a message.

## Scripts in this folder

Representative studies:
- `transformer_utilisation_by_demand.py`
- `pv_percent_by_transformer.py`
- `suspect_end_of_line.py`
- `transformer_downstream_density.py`
- `customer_distance_to_transformer.py`
- `loop_impedance_by_energy_consumer.py`
- `tap_changer_info_by_transformer.py`

See each script’s header and help output for specifics:

```bash
python <script>.py --help
```
