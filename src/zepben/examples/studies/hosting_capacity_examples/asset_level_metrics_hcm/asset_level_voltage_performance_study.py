#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Set, Optional

from geojson import Feature, FeatureCollection
from zepben.eas import GeoJsonOverlayInput, Mutation, StudyInput, StudyResultInput
from zepben.examples.studies.study_utils import create_eas_client_for_host
from zepben.ewb import ConductingEquipment, Feeder, IncludedEnergizedContainers, NetworkConsumerClient

try:
    from zepben.examples.studies.hosting_capacity_examples.asset_level_metrics_hcm.common import (
        VoltageAssetWorstCase,
        expand_simplified_asset_mrids,
        load_cim_to_opendss_mapping,
        load_voltage_asset_summaries,
        voltage_bandwidth_bucket,
        voltage_high_bucket,
        voltage_low_bucket,
        worst_voltage_by_asset,
    )
    from zepben.examples.studies.hosting_capacity_examples.common import (
        _connect_rpc,
        create_postgres_engine,
        load_db_settings,
        load_ewb_settings,
        split_csv_values,
        to_equipment_geometry,
    )
except ModuleNotFoundError:
    from common import (  # type: ignore
        VoltageAssetWorstCase,
        expand_simplified_asset_mrids,
        load_cim_to_opendss_mapping,
        load_voltage_asset_summaries,
        voltage_bandwidth_bucket,
        voltage_high_bucket,
        voltage_low_bucket,
        worst_voltage_by_asset,
    )
    from zepben.examples.studies.hosting_capacity_examples.common import (  # type: ignore
        _connect_rpc,
        create_postgres_engine,
        load_db_settings,
        load_ewb_settings,
        split_csv_values,
        to_equipment_geometry,
    )


BASE_DIR = Path(__file__).resolve().parent
STYLE_PATH = BASE_DIR / "style_asset_level_metrics_hcm.json"

VOLTAGE_METRICS = (
    ("low_voltage_lg_v", "Lowest Endpoint Voltage (V L-G)", "Low V"),
    ("high_voltage_lg_v", "Highest Endpoint Voltage (V L-G)", "High V"),
    ("bandwidth_pct", "Endpoint Voltage Bandwidth (%)", "Bandwidth"),
    ("limit_hours", "Voltage Limit Hours", "Limit Hours"),
)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload an asset-level voltage performance study from the hosting capacity "
            "Postgres result tables."
        )
    )
    parser.add_argument("--work-package-id", required=True, help="Result work package ID.")
    parser.add_argument("--scenario", default="", help="Optional scenario filter.")
    parser.add_argument("--year", type=int, help="Optional result year filter.")
    parser.add_argument("--feeders", default="", help="Comma-separated result feeder codes/MRIDs to include.")
    parser.add_argument(
        "--feeder-mrids",
        default="",
        help="Comma-separated EWB feeder MRIDs. Overrides hierarchy lookup from result feeder codes.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env with EWB_* connection details and RESULT_DB_* settings.",
    )
    parser.add_argument("--name", default="Asset Level HCM Voltage Performance", help="Study name.")
    parser.add_argument("--dry-run", action="store_true", help="Generate study payload but do not upload.")
    return parser.parse_args(argv)


async def main(argv: Sequence[str]) -> None:
    args = parse_args(argv)
    ewb_settings = load_ewb_settings(args.env_file)
    db_settings = load_db_settings(args.env_file)
    work_package_id = args.work_package_id.strip()
    feeders = split_csv_values(args.feeders)
    engine = create_postgres_engine(db_settings)

    selected_rows = load_voltage_asset_summaries(
        engine,
        work_package_id=work_package_id,
        scenario=args.scenario.strip() or None,
        year=args.year,
        feeders=feeders,
    )
    if not selected_rows:
        raise ValueError("No voltage rows matched the requested filters.")

    mapping = load_cim_to_opendss_mapping(
        engine,
        work_package_id=work_package_id,
        scenario=args.scenario.strip() or None,
        year=args.year,
        feeders=feeders,
    )
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

    worst_by_asset = worst_voltage_by_asset(selected_rows)
    results: List[StudyResultInput] = []
    for metric_key, result_name, label_prefix in VOLTAGE_METRICS:
        features = build_voltage_features(
            worst_by_asset=worst_by_asset,
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
                    styles=[f"asset-voltage-{metric_key}-line", f"asset-voltage-{metric_key}-point", f"asset-voltage-{metric_key}-label"],
                ),
            )
        )

    study = StudyInput(
        name=args.name,
        description=(
            "Asset-level HCM voltage performance layers sourced from "
            "public.asset_level_voltage_summary. Phase rows are collapsed to worst low voltage, "
            "worst high voltage, endpoint bandwidth, and total voltage limit hours. Result MRIDs "
            "are expanded through public.cim_to_opendss where a simplified OpenDSS asset maps "
            "back to one or more original CIM assets."
        ),
        tags=[
            "hosting_capacity",
            "asset_level_metrics",
            "voltage_performance",
            "voltage_bandwidth",
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
        print("Uploading voltage performance study...")
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


def build_voltage_features(
    *,
    worst_by_asset: Dict[str, VoltageAssetWorstCase],
    simplified_to_original: Dict[str, Sequence[str]],
    equipment_by_mrid: Dict[str, object],
    metric_key: str,
    label_prefix: str,
) -> List[Feature]:
    features: List[Feature] = []
    missing_assets: Set[str] = set()
    for result_asset_mrid, summary in worst_by_asset.items():
        value = voltage_metric_value(summary, metric_key)
        if value is None:
            continue

        for asset_mrid in candidate_asset_mrids(
            result_asset_mrid=result_asset_mrid,
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

            bucket = voltage_metric_bucket(metric_key, value)
            features.append(
                Feature(
                    id=f"{metric_key}:{result_asset_mrid}:{asset_mrid}",
                    geometry=geometry,
                    properties={
                        "metric_key": metric_key,
                        "geom_kind": geom_kind,
                        "asset_mrid": asset_mrid,
                        "result_asset_mrid": result_asset_mrid,
                        "metric_value": round(value, 6),
                        "value_label": (
                            voltage_value_label_from_summary(summary, value, label_prefix)
                            if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"}
                            else voltage_value_label(metric_key, value, label_prefix)
                        ),
                        "voltage_pu": (
                            round(voltage_pu(summary, value), 6)
                            if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"}
                            else None
                        ),
                        "voltage_pu_delta_pct": (
                            round(voltage_pu_delta_pct(summary, value), 6)
                            if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"}
                            else None
                        ),
                        "display_voltage_value": (
                            round(display_voltage_value(summary, value), 6)
                            if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"}
                            else None
                        ),
                        "display_voltage_unit": (
                            "kV" if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"} and (summary.v_base or 0) >= 1000 else (
                                "V" if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"} else None
                            )
                        ),
                        "display_voltage_reference": (
                            "L-L" if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"} and (summary.v_base or 0) >= 1000 else (
                                "L-G" if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"} else None
                            )
                        ),
                        "bucket": bucket,
                        "bucket_label": voltage_bucket_label(metric_key, bucket),
                        "low_voltage_lg_v": round(summary.low_voltage_lg_v, 6),
                        "low_phase": summary.low_phase,
                        "high_voltage_lg_v": round(summary.high_voltage_lg_v, 6),
                        "high_phase": summary.high_phase,
                        "bandwidth_pct": round(summary.bandwidth_pct, 6),
                        "bandwidth_lg_v": round(summary.bandwidth_lg_v, 6),
                        "bandwidth_phase": summary.bandwidth_phase,
                        "hours_below_limit": round(summary.hours_below_limit, 6),
                        "hours_above_limit": round(summary.hours_above_limit, 6),
                        "limit_hours": round(summary.hours_below_limit + summary.hours_above_limit, 6),
                        "phase_row_count": summary.row_count,
                        "v_base": summary.v_base,
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


def voltage_metric_value(summary: VoltageAssetWorstCase, metric_key: str) -> Optional[float]:
    if metric_key == "low_voltage_lg_v":
        return summary.low_voltage_lg_v
    if metric_key == "high_voltage_lg_v":
        return summary.high_voltage_lg_v
    if metric_key == "bandwidth_pct":
        return summary.bandwidth_pct
    if metric_key == "limit_hours":
        return summary.hours_below_limit + summary.hours_above_limit
    raise ValueError(f"Unsupported voltage metric: {metric_key}")


def voltage_metric_bucket(metric_key: str, value: float) -> int:
    if metric_key == "low_voltage_lg_v":
        return voltage_low_bucket(value)
    if metric_key == "high_voltage_lg_v":
        return voltage_high_bucket(value)
    if metric_key == "bandwidth_pct":
        return voltage_bandwidth_bucket(value)
    return limit_hours_bucket(value)


def voltage_bucket_label(metric_key: str, bucket: int) -> str:
    if metric_key == "low_voltage_lg_v":
        return {0: "Normal", 1: "Watch", 2: "Low", 3: "Below limit"}.get(bucket, "Unknown")
    if metric_key == "high_voltage_lg_v":
        return {0: "Normal", 1: "Watch", 2: "High", 3: "Above limit"}.get(bucket, "Unknown")
    if metric_key == "bandwidth_pct":
        return {0: "Tight", 1: "Moderate", 2: "Wide", 3: "Very wide"}.get(bucket, "Unknown")
    return {0: "None", 1: "Low", 2: "Elevated", 3: "High"}.get(bucket, "Unknown")


def limit_hours_bucket(value: Optional[float]) -> int:
    if value is None or value <= 0:
        return 0
    if value < 10.0:
        return 1
    if value < 100.0:
        return 2
    return 3


def voltage_value_label(metric_key: str, value: float, label_prefix: str) -> str:
    if metric_key in {"low_voltage_lg_v", "high_voltage_lg_v"}:
        return f"{label_prefix}: {value:.1f} V"
    if metric_key == "bandwidth_pct":
        return f"{label_prefix}: {value:.2f}%"
    return f"{label_prefix}: {value:.1f} h"


def display_voltage_value(summary: VoltageAssetWorstCase, raw_voltage_lg_v: float) -> float:
    if summary.v_base is not None and summary.v_base >= 1000:
        return raw_voltage_lg_v * math.sqrt(3.0) / 1000.0
    return raw_voltage_lg_v


def voltage_pu(summary: VoltageAssetWorstCase, raw_voltage_lg_v: float) -> float:
    if summary.v_base is None or summary.v_base <= 0:
        return float("nan")
    return raw_voltage_lg_v * math.sqrt(3.0) / float(summary.v_base)


def voltage_pu_delta_pct(summary: VoltageAssetWorstCase, raw_voltage_lg_v: float) -> float:
    return (voltage_pu(summary, raw_voltage_lg_v) - 1.0) * 100.0


def voltage_value_label_from_summary(summary: VoltageAssetWorstCase, raw_voltage_lg_v: float, label_prefix: str) -> str:
    if summary.v_base is not None and summary.v_base >= 1000:
        return f"{label_prefix}: {display_voltage_value(summary, raw_voltage_lg_v):.1f} kV (L-L)"
    return f"{label_prefix}: {raw_voltage_lg_v:.1f} V (L-G)"


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
