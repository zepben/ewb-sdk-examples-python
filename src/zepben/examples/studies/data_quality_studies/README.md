# Data Quality Studies

These scripts generate data-quality analysis layers focused on connectivity and power-flow modeling issues.
They upload EAS studies using the credentials in `src/zepben/examples/config.json`.

## Usage

From the repository root:

```
python src/zepben/examples/studies/data_quality_studies/connectivity_gaps.py CPM
python src/zepben/examples/studies/data_quality_studies/consumer_mapping_issues.py CPM
python src/zepben/examples/studies/data_quality_studies/phase_conductor_issues.py CPM
python src/zepben/examples/studies/data_quality_studies/asset_attribute_inconsistencies.py CPM
python src/zepben/examples/studies/data_quality_studies/protection_directionality_anomalies.py CPM
python src/zepben/examples/studies/data_quality_studies/spatial_location_anomalies.py CPM
```

Use a comma-separated list for multiple zones:

```
python src/zepben/examples/studies/data_quality_studies/connectivity_gaps.py CPM,NSK
```

## Summary Study

`data_quality_summary.py` runs all checks for a zone and uploads a single study
containing only layers where anomalies are detected. The study description and
tags list only the tests that reported anomalies.

```
python src/zepben/examples/studies/data_quality_studies/data_quality_summary.py NSK
```

## Notes

- Each script accepts a zone code argument. If omitted, it defaults to `CPM`.
- Individual scripts always upload all layers; if no anomalies are found, the
  layer name is prefixed with "No anomalies detected: ...".
- The summary script skips upload if no anomalies are detected at all.
