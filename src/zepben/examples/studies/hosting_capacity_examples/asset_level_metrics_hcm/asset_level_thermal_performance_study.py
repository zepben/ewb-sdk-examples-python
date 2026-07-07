#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set

from geojson import Feature, FeatureCollection
from zepben.eas import GeoJsonOverlayInput, Mutation, StudyInput, StudyResultInput
from zepben.examples.studies.study_utils import create_eas_client_for_host
from zepben.ewb import ConductingEquipment, Feeder, IncludedEnergizedContainers, NetworkConsumerClient

try:
    from zepben.examples.studies.hosting_capacity_examples.asset_level_metrics_hcm.common import (
        ThermalAssetSummary,
        expand_simplified_asset_mrids,
        filter_records,
        load_cim_to_opendss_mapping,
        load_thermal_asset_summaries,
        parse_float,
        thermal_loading_bucket,
        thermal_loading_label,
    )
    from zepben.examples.studies.hosting_capacity_examples.common import (
        _connect_rpc,
        load_ewb_settings,
        split_csv_values,
        to_equipment_geometry,
    )
except ModuleNotFoundError:
    from common import (  # type: ignore
        ThermalAssetSummary,
        expand_simplified_asset_mrids,
        filter_records,
        load_cim_to_opendss_mapping,
        load_thermal_asset_summaries,
        parse_float,
        thermal_loading_bucket,
        thermal_loading_label,
    )
    from zepben.examples.studies.hosting_capacity_examples.common import (  # type: ignore
        _connect_rpc,
        load_ewb_settings,
        split_csv_values,
        to_equipment_geometry,
    )


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_RESULTS_DIR = BASE_DIR / "example_asset_level_results"
STYLE_PATH = BASE_DIR / "style_asset_level_metrics_hcm.json"

THERMAL_METRICS = (
    ("max_loading_pct", "Maximum Loading (%)", "Max Loading"),
    ("hours_over_normal", "Hours Over Normal Rating", "Hours > Normal"),
    ("hours_over_emergency", "Hours Over Emergency Rating", "Hours > Emergency"),
    ("overload_kwh_import", "Import Overload Energy (kWh)", "Import kWh"),
    ("overload_kwh_export", "Export Overload Energy (kWh)", "Export kWh"),
)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a prototype asset-level thermal performance study from "
            "thermal_results_base.csv and cim_to_opendss_base.csv."
        )
    )
    parser.add_argument("--results-dir", default=str(DEFAULT_RESULTS_DIR), help="Directory containing prototype CSV outputs.")
    parser.add_argument("--thermal-results", default="", help="Thermal results CSV override.")
    parser.add_argument("--mapping", default="", help="CIM/OpenDSS mapping CSV override.")
    parser.add_argument("--work-package-id", default="", help="Optional work package filter.")
    parser.add_argument("--scenario", default="", help="Optional scenario filter.")
    parser.add_argument("--year", type=int, help="Optional result year filter.")
    parser.add_argument("--feeders", default="", help="Comma-separated result feeder codes/MRIDs to include.")
    parser.add_argument(
        "--feeder-mrids",
        default="",
        help="Comma-separated EWB feeder MRIDs. Overrides hierarchy lookup from result feeder codes.",
    )
    parser.add_argument("--env-file", default=".env", help="Path to .env with EWB_* connection details.")
    parser.add_argument("--name", default="Asset Level HCM Thermal Performance", help="Study name.")
    parser.add_argument("--dry-run", action="store_true", help="Generate study payload but do not upload.")
    return parser.parse_args(argv)


async def main(argv: Sequence[str]) -> None:
    args = parse_args(argv)
    results_dir = Path(args.results_dir)
    thermal_path = Path(args.thermal_results) if args.thermal_results else results_dir / "thermal_results_base.csv"
    mapping_path = Path(args.mapping) if args.mapping else results_dir / "cim_to_opendss_base.csv"

    ewb_settings = load_ewb_settings(args.env_file)
    rows = load_thermal_asset_summaries(thermal_path)
    selected_rows = filter_records(
        rows=rows,
        work_package_id=args.work_package_id.strip() or None,
        scenario=args.scenario.strip() or None,
        year=args.year,
        feeders=split_csv_values(args.feeders),
    )
    if not selected_rows:
        raise ValueError("No thermal rows matched the requested filters.")

    mapping = load_cim_to_opendss_mapping(mapping_path)
    result_feeders = sorted({row.feeder for row in selected_rows})
    feeder_mrids = await resolve_feeder_mrids(
        ewb_settings=ewb_settings,
        result_feeders=result_feeders,
        explicit_feeder_mrids=split_csv_values(args.feeder_mrids),
    )
    if not feeder_mrids:
        raise ValueError(
            "No EWB feeder MRIDs resolved. Provide --feeder-mrids if the CSV feeder codes do not match EWB names."
        )

    equipment_by_mrid = await fetch_equipment_by_mrid(ewb_settings, feeder_mrids)
    print(f"Loaded {len(equipment_by_mrid)} equipment object(s) from {len(feeder_mrids)} feeder(s).")

    results: List[StudyResultInput] = []
    for metric_key, result_name, label_prefix in THERMAL_METRICS:
        features = build_thermal_features(
            rows=selected_rows,
            simplified_to_original=mapping,
            equipment_by_mrid=equipment_by_mrid,
            metric_key=metric_key,
            label_prefix=label_prefix,
        )
        print(f"  - {result_name}: {len(features)} feature(s)")
        results.append(
            StudyResultInput(
                name=result_name,
                sections=[],
                geo_json_overlay=GeoJsonOverlayInput(
                    data=FeatureCollection(features),
                    styles=[f"asset-thermal-{metric_key}-line", f"asset-thermal-{metric_key}-point", f"asset-thermal-{metric_key}-label"],
                ),
            )
        )

    study = StudyInput(
        name=args.name,
        description=(
            "Prototype asset-level HCM thermal performance layers sourced from thermal_results_base.csv. "
            "Result MRIDs are expanded through cim_to_opendss_base.csv so simplified OpenDSS assets render on "
            "their original CIM assets where a mapping exists."
        ),
        tags=[
            "hosting_capacity",
            "asset_level_metrics",
            "thermal_performance",
            *[f"feeder_{feeder}" for feeder in result_feeders[:10]],
        ],
        results=results,
        styles=json.loads(STYLE_PATH.read_text()),
    )

    if args.dry_run:
        print("Dry-run enabled. Study was not uploaded.")
        return

    eas_client = create_eas_client_for_host(
        host=ewb_settings.host,
        port=ewb_settings.rpc_port,
        access_token=ewb_settings.access_token,
        ca_filename=ewb_settings.ca_filename,
    )
    try:
        print("Uploading thermal performance study...")
        response = await eas_client.mutation(Mutation.add_studies(studies=[study]))
        print(f"Study upload response: {response}")
    finally:
        await eas_client.close()


async def resolve_feeder_mrids(ewb_settings, result_feeders: Sequence[str], explicit_feeder_mrids: Sequence[str]) -> List[str]:
    if explicit_feeder_mrids:
        return sorted(set(explicit_feeder_mrids))

    rpc_channel = _connect_rpc(ewb_settings)
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()

    requested = set(result_feeders)
    resolved: Set[str] = set()
    resolved_codes: Set[str] = set()
    for substation in hierarchy.value.substations.values():
        for feeder in substation.feeders:
            names = {
                str(getattr(feeder, "mrid", "") or ""),
                str(getattr(feeder, "name", "") or ""),
                str(getattr(feeder, "description", "") or ""),
            }
            matched = requested.intersection(names)
            if matched:
                resolved.add(str(feeder.mrid))
                resolved_codes.update(matched)

    missing = sorted(requested - resolved_codes)
    if missing:
        print(
            "Could not resolve these result feeder codes directly to EWB feeder names/MRIDs: "
            + ", ".join(missing)
        )
    return sorted(resolved)


async def fetch_equipment_by_mrid(ewb_settings, feeder_mrids: Sequence[str]):
    equipment_by_mrid: Dict[str, object] = {}
    rpc_channel = _connect_rpc(ewb_settings)
    client = NetworkConsumerClient(rpc_channel)

    for feeder_mrid in feeder_mrids:
        print(f"Fetching feeder {feeder_mrid}")
        result = await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
        )
        if result.was_failure:
            print(f"  - failed: {getattr(result, 'thrown', result)}")
            continue
        for equipment in client.service.objects(ConductingEquipment):
            mrid = getattr(equipment, "mrid", None)
            if mrid:
                equipment_by_mrid[str(mrid)] = equipment

    return equipment_by_mrid


def build_thermal_features(
    *,
    rows: Sequence[ThermalAssetSummary],
    simplified_to_original: Dict[str, Sequence[str]],
    equipment_by_mrid: Dict[str, object],
    metric_key: str,
    label_prefix: str,
) -> List[Feature]:
    features: List[Feature] = []
    missing_assets: Set[str] = set()
    for row in rows:
        value = parse_float(getattr(row, metric_key))
        if value is None:
            continue

        for asset_mrid in candidate_asset_mrids(
            result_asset_mrid=row.conducting_equipment_mrid,
            simplified_to_original=simplified_to_original,
            equipment_by_mrid=equipment_by_mrid,
        ):
            equipment = equipment_by_mrid.get(asset_mrid)
            if equipment is None:
                missing_assets.add(asset_mrid)
                continue

            geom_kind, geometry = to_equipment_geometry(equipment)
            if geometry is None:
                continue

            bucket = thermal_loading_bucket(row.max_loading_pct) if metric_key == "max_loading_pct" else exceedance_bucket(value)
            value_label = thermal_value_label(metric_key, value, label_prefix)
            features.append(
                Feature(
                    id=f"{metric_key}:{row.conducting_equipment_mrid}:{asset_mrid}",
                    geometry=geometry,
                    properties={
                        "metric_key": metric_key,
                        "geom_kind": geom_kind,
                        "asset_mrid": asset_mrid,
                        "result_asset_mrid": row.conducting_equipment_mrid,
                        "feeder": row.feeder,
                        "scenario": row.scenario,
                        "year": row.year,
                        "metric_value": round(value, 6),
                        "value_label": value_label,
                        "bucket": bucket,
                        "bucket_label": (
                            thermal_loading_label(bucket)
                            if metric_key == "max_loading_pct"
                            else exceedance_bucket_label(bucket)
                        ),
                        "rating_unit": row.rating_unit,
                        "normal_rating": row.normal_rating,
                        "emergency_rating": row.emergency_rating,
                        "max_loading_pct": row.max_loading_pct,
                        "direction_at_max_loading": row.direction_at_max_loading,
                        "hours_over_normal": row.hours_over_normal,
                        "hours_over_emergency": row.hours_over_emergency,
                        "overload_kwh_import": row.overload_kwh_import,
                        "overload_kwh_export": row.overload_kwh_export,
                        "worst_phase": row.worst_phase,
                    },
                )
            )

    if missing_assets:
        preview = ", ".join(sorted(missing_assets)[:10])
        suffix = "..." if len(missing_assets) > 10 else ""
        print(f"  - missing mapped assets for {metric_key}: {len(missing_assets)} ({preview}{suffix})")
    return features


def candidate_asset_mrids(
    *,
    result_asset_mrid: str,
    simplified_to_original: Dict[str, Sequence[str]],
    equipment_by_mrid: Dict[str, object],
) -> Sequence[str]:
    mapped = expand_simplified_asset_mrids(result_asset_mrid, simplified_to_original)
    available_mapped = tuple(asset_mrid for asset_mrid in mapped if asset_mrid in equipment_by_mrid)
    if available_mapped:
        return available_mapped
    if result_asset_mrid in equipment_by_mrid:
        return (result_asset_mrid,)
    return mapped


def exceedance_bucket(value: Optional[float]) -> int:
    if value is None or value <= 0:
        return 0
    if value < 100.0:
        return 1
    if value < 1000.0:
        return 2
    return 3


def exceedance_bucket_label(bucket: int) -> str:
    return {
        0: "None",
        1: "Low",
        2: "Elevated",
        3: "High",
    }.get(bucket, "Unknown")


def thermal_value_label(metric_key: str, value: float, label_prefix: str) -> str:
    if metric_key == "max_loading_pct":
        return f"{label_prefix}: {value:.1f}%"
    if metric_key.startswith("hours_"):
        return f"{label_prefix}: {value:.1f} h"
    return f"{label_prefix}: {value:.1f} kWh"


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
