# Zone Substation Analytics Demo

This folder contains a correlated analytics demo study uploader for regional network-performance review.

The demo is designed for overview workflows at zone-substation level and intentionally keeps drill-down dashboards out of scope.
Each feature includes a `dashboard_key` property so external dashboards can be linked later.
The generated values are synthetic, but correlated to live EWB network signals (topology, customer counts, PV/PEC penetration, phase spread, impedance proxy, inferred loading/headroom).

## Script

- `mock_zone_analytics_study.py`

## Included use-case layers

The script currently publishes one result per selected use case:

1. Neutral Integrity Fault Detection
2. Voltage Monitoring and Reporting
3. Dynamic Voltage Control
4. Phase Identification
5. CER Compliance
6. CER Performance
7. EV Charger Detection

Notes:
- Dynamic Voltage Control mock logic uses ADMD `4kW` and ADMG `5kW` per customer for tap-zone voltage-drop estimates on MV lines.
- MV/LV loading and New connection assessment layers were intentionally removed from this demo.
- CER detection was intentionally removed in this version.

Each result maps to dedicated styles in `style_mock_zone_analytics.json`, so every use case appears as its own selectable layer in the study sidebar/legend.

## Run

From `src/zepben/examples/studies/analytics_demo_examples`:

```bash
python mock_zone_analytics_study.py
```

Optional args:

```bash
python mock_zone_analytics_study.py --zones CPM --seed 42
python mock_zone_analytics_study.py --dry-run
python mock_zone_analytics_study.py --period-start 2025-01-01 --period-end 2025-12-31
```

The script reads credentials from `src/zepben/examples/config.json` by default.
