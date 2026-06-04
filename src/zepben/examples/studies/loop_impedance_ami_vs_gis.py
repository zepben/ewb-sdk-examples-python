"""
Create a study that compares loop impedance values from GIS (`R_gis`) with AMI-derived
estimates (`R_est`) and visualises the relative error (`R_error`) on customer locations.

Matching rules
--------------
- The CSV `NMI` column is matched to EWB `UsagePoint.names` where `Name.type.name == "NMI"`.
- If duplicate CSV rows exist for the same NMI, the rows are treated as per-phase values and
  averaged into a single customer result for `R_gis`, `R_est`, and `R_error`.

Study results
-------------
1. `R_error` (first result, colour thresholds derived from the supplied dataset quantiles)
2. `R_gis`
3. `R_est`
"""

import argparse
import asyncio
import csv
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry, Point
from zepben.eas import GeoJsonOverlayInput, Mutation, StudyInput, StudyResultInput
from zepben.examples.studies.study_utils import (
    EXAMPLES_CONFIG_PATH,
    connect_rpc_from_config,
    create_eas_client_from_config,
    load_examples_config,
)
from zepben.ewb import EnergyConsumer, Feeder, IncludedEnergizedContainers, Location, NetworkConsumerClient

DEFAULT_CSV_PATH = Path(__file__).resolve().parent / "loop_impedance" / "ue-loop-impedance-errors-tx-91165628.csv"
STYLE_PATH = Path(__file__).resolve().parent / "style_loop_impedance_ami_vs_gis.json"
DEFAULT_BATCH_SIZE = 3

# Rounded thresholds chosen after reviewing the supplied CSV distribution.
# R_error quantiles from the dataset were approximately:
# p10=0.040, p25=0.074, p50=0.142, p75=0.307, p90=0.520, p95=0.592.
ERROR_THRESHOLDS = (0.04, 0.08, 0.15, 0.30, 0.55, 1.00)


@dataclass(frozen=True)
class CsvLoopImpedanceRow:
    nmi: str
    r_gis: float
    r_est: float
    r_error: float
    source_row_count: int = 1


@dataclass(frozen=True)
class MatchedLoopImpedancePoint:
    nmi: str
    energy_consumer: EnergyConsumer
    r_gis: float
    r_est: float
    r_error: float
    source_row_count: int
    network_match_count: int


def chunk(it: Iterable[str], size: int):
    iterator = iter(it)
    return iter(lambda: tuple(islice(iterator, size)), ())


async def main() -> None:
    zone_mrids, feeder_mrids, mode, csv_path, config_path, study_name = _parse_args(sys.argv[1:])
    config = load_examples_config(config_path)
    csv_rows_by_nmi, total_csv_rows, duplicate_nmi_count = _load_csv_rows(csv_path)

    print(f"Loaded {total_csv_rows} CSV rows across {len(csv_rows_by_nmi)} unique NMIs from {csv_path}")
    print(f"Duplicate CSV NMI count: {duplicate_nmi_count} (per-NMI values averaged across duplicate rows)")
    print(f"R_error thresholds (quantile-informed): {', '.join(f'{value:.2f}' for value in ERROR_THRESHOLDS)}")

    scope_label, scope_tag, feeder_mrids = await _resolve_scope(
        config=config,
        zone_mrids=zone_mrids,
        feeder_mrids=feeder_mrids,
        mode=mode,
    )

    if not feeder_mrids:
        print("No feeders resolved for the requested scope. Nothing to process.")
        return

    matched_points_by_nmi: Dict[str, MatchedLoopImpedancePoint] = {}
    network_match_counts: Dict[str, int] = defaultdict(int)

    for feeders in chunk(feeder_mrids, DEFAULT_BATCH_SIZE):
        rpc_channel = connect_rpc_from_config(config)
        print(f"Processing feeders {', '.join(feeders)}")
        futures = [
            asyncio.ensure_future(
                _fetch_feeder_matches(
                    feeder_mrid=feeder_mrid,
                    rpc_channel=rpc_channel,
                    csv_rows_by_nmi=csv_rows_by_nmi,
                )
            )
            for feeder_mrid in feeders
        ]

        for future in futures:
            feeder_matches = await future
            for nmi, energy_consumer in feeder_matches:
                network_match_counts[nmi] += 1
                csv_row = csv_rows_by_nmi[nmi]
                candidate = MatchedLoopImpedancePoint(
                    nmi=nmi,
                    energy_consumer=energy_consumer,
                    r_gis=csv_row.r_gis,
                    r_est=csv_row.r_est,
                    r_error=csv_row.r_error,
                    source_row_count=csv_row.source_row_count,
                    network_match_count=network_match_counts[nmi],
                )
                existing = matched_points_by_nmi.get(nmi)
                if existing is None or _prefer_consumer(candidate.energy_consumer, existing.energy_consumer):
                    matched_points_by_nmi[nmi] = candidate

    for nmi, point in list(matched_points_by_nmi.items()):
        match_count = network_match_counts.get(nmi, point.network_match_count)
        if match_count == point.network_match_count:
            continue
        matched_points_by_nmi[nmi] = MatchedLoopImpedancePoint(
            nmi=point.nmi,
            energy_consumer=point.energy_consumer,
            r_gis=point.r_gis,
            r_est=point.r_est,
            r_error=point.r_error,
            source_row_count=point.source_row_count,
            network_match_count=match_count,
        )

    feature_collection = _build_feature_collection(list(matched_points_by_nmi.values()))
    if not feature_collection.features:
        print("No mappable customer features were found for the requested scope. Study upload skipped.")
        return

    matched_nmis = set(matched_points_by_nmi.keys())
    unmatched_nmis = sorted(set(csv_rows_by_nmi.keys()) - matched_nmis)
    print(
        "CSV match summary: "
        f"matched={len(matched_nmis)}, "
        f"unmatched={len(unmatched_nmis)}, "
        f"mappable_features={len(feature_collection.features)}"
    )
    if unmatched_nmis:
        preview = ", ".join(unmatched_nmis[:10])
        suffix = "..." if len(unmatched_nmis) > 10 else ""
        print(f"First unmatched NMIs: {preview}{suffix}")

    styles = json.loads(STYLE_PATH.read_text())
    results = [
        StudyResultInput(
            name=f"R_error ({len(feature_collection.features)})",
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(
                data=feature_collection,
                styles=["loop-impedance-error-circle", "loop-impedance-error-label"],
            ),
        ),
        StudyResultInput(
            name=f"R_gis ({len(feature_collection.features)})",
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(
                data=feature_collection,
                styles=["loop-impedance-r-gis-circle", "loop-impedance-r-gis-label"],
            ),
        ),
        StudyResultInput(
            name=f"R_est ({len(feature_collection.features)})",
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(
                data=feature_collection,
                styles=["loop-impedance-r-est-circle", "loop-impedance-r-est-label"],
            ),
        ),
    ]

    eas_client = create_eas_client_from_config(config)
    resolved_study_name = study_name or f"Loop impedance AMI vs GIS ({scope_label})"
    print(f"Uploading Study for {scope_label} ...")
    await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=resolved_study_name,
            description=(
                "Loop impedance comparison using CSV values matched to EWB UsagePoint NMI names. "
                "Where duplicate CSV rows existed for the same NMI, per-phase values were averaged into a single customer result. "
                f"Matched {len(matched_nmis)} of {len(csv_rows_by_nmi)} CSV NMIs for scope {scope_label}; "
                f"unmatched={len(unmatched_nmis)}."
            ),
            tags=["loop_impedance", "ami_vs_gis", scope_tag],
            results=results,
            styles=styles,
        )
    ]))
    await eas_client.close()
    print("Uploaded Study")


def _parse_args(argv: Sequence[str]) -> Tuple[List[str], List[str], str, str, Optional[str], str]:
    parser = argparse.ArgumentParser(
        description=(
            "Create a study showing R_error, R_gis and R_est values from a loop-impedance CSV. "
            "CSV NMI values are matched to UsagePoint NMI names in the EWB model."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["zones", "feeders"],
        default=None,
        help=(
            "Run by zones or feeders. If omitted, --feeders implies feeders mode; "
            "otherwise zones mode is used."
        ),
    )
    parser.add_argument(
        "--zones",
        default="CPM",
        help="Comma-separated zone codes (default: CPM).",
    )
    parser.add_argument(
        "--feeders",
        "--feeder",
        default="",
        help="Comma-separated feeder MRIDs (used when --mode feeders).",
    )
    parser.add_argument(
        "--csv",
        default=str(DEFAULT_CSV_PATH),
        help=f"Path to the loop impedance CSV (default: {DEFAULT_CSV_PATH}).",
    )
    parser.add_argument(
        "--config",
        default=None,
        help=f"Optional path to config.json (default: {EXAMPLES_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--study-name",
        default="",
        help="Optional override for the uploaded study name.",
    )
    parser.add_argument(
        "ids",
        nargs="*",
        help="Zone codes or feeder MRIDs (positional values override --zones/--feeders).",
    )
    args = parser.parse_args(argv)

    def _split(values: Union[str, Sequence[str]]) -> List[str]:
        if isinstance(values, str):
            items = values.split(",")
        else:
            items = list(values)
        return [item.strip() for item in items if item and item.strip()]

    feeders_from_flag = _split(args.feeders)
    effective_mode = args.mode or ("feeders" if feeders_from_flag else "zones")

    if effective_mode == "feeders":
        feeders = _split(args.ids) or feeders_from_flag
        if not feeders:
            raise ValueError("At least one feeder MRID is required in feeder mode.")
        return [], feeders, effective_mode, args.csv, args.config, args.study_name

    zones = _split(args.ids) or _split(args.zones)
    if not zones:
        raise ValueError("At least one zone code is required in zone mode.")
    return zones, [], effective_mode, args.csv, args.config, args.study_name


def _load_csv_rows(csv_path: str) -> Tuple[Dict[str, CsvLoopImpedanceRow], int, int]:
    sums_by_nmi: Dict[str, Dict[str, float]] = defaultdict(lambda: {
        "r_gis": 0.0,
        "r_est": 0.0,
        "r_error": 0.0,
    })
    counts_by_nmi: Dict[str, int] = defaultdict(int)
    total_rows = 0

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_rows += 1
            nmi = str(row["NMI"]).strip()
            sums_by_nmi[nmi]["r_gis"] += float(row["R_gis"])
            sums_by_nmi[nmi]["r_est"] += float(row["R_est"])
            sums_by_nmi[nmi]["r_error"] += float(row["R_error"])
            counts_by_nmi[nmi] += 1

    rows_by_nmi: Dict[str, CsvLoopImpedanceRow] = {}
    for nmi, sums in sums_by_nmi.items():
        count = counts_by_nmi[nmi]
        rows_by_nmi[nmi] = CsvLoopImpedanceRow(
            nmi=nmi,
            r_gis=sums["r_gis"] / count,
            r_est=sums["r_est"] / count,
            r_error=sums["r_error"] / count,
            source_row_count=count,
        )

    duplicate_nmi_count = sum(1 for count in counts_by_nmi.values() if count > 1)
    return rows_by_nmi, total_rows, duplicate_nmi_count


async def _resolve_scope(
    *,
    config: Dict,
    zone_mrids: List[str],
    feeder_mrids: List[str],
    mode: str,
) -> Tuple[str, str, List[str]]:
    if mode == "feeders":
        return ", ".join(feeder_mrids), "-".join(feeder_mrids), feeder_mrids

    rpc_channel = connect_rpc_from_config(config)
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()
    substations = hierarchy.value.substations

    resolved_feeders: List[str] = []
    for zone_mrid in zone_mrids:
        if zone_mrid not in substations:
            continue
        for feeder in substations[zone_mrid].feeders:
            resolved_feeders.append(feeder.mrid)

    return ", ".join(zone_mrids), "-".join(zone_mrids), resolved_feeders


async def _fetch_feeder_matches(
    *,
    feeder_mrid: str,
    rpc_channel,
    csv_rows_by_nmi: Dict[str, CsvLoopImpedanceRow],
) -> List[Tuple[str, EnergyConsumer]]:
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)
    result = await client.get_equipment_container(
        mrid=feeder_mrid,
        expected_class=Feeder,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
    )
    if result.was_failure:
        print(f"Failed feeder {feeder_mrid}: {getattr(result, 'thrown', result)}")
        return []

    print(f"Finished fetching Feeder {feeder_mrid}")
    network = client.service

    matches: List[Tuple[str, EnergyConsumer]] = []
    for energy_consumer in network.objects(EnergyConsumer):
        matched_nmis = _matching_nmis(energy_consumer, csv_rows_by_nmi)
        for nmi in matched_nmis:
            matches.append((nmi, energy_consumer))

    return matches


def _matching_nmis(
    energy_consumer: EnergyConsumer,
    csv_rows_by_nmi: Dict[str, CsvLoopImpedanceRow],
) -> Set[str]:
    matched_nmis: Set[str] = set()
    for usage_point in energy_consumer.usage_points:
        for candidate in _usage_point_nmi_candidates(usage_point):
            if candidate in csv_rows_by_nmi:
                matched_nmis.add(candidate)
    return matched_nmis


def _usage_point_nmi_candidates(usage_point) -> Set[str]:
    candidates: Set[str] = set()

    for name in getattr(usage_point, "names", []) or []:
        name_type = getattr(getattr(name, "type", None), "name", None)
        value = getattr(name, "name", None)
        if value and name_type == "NMI":
            candidates.add(str(value).strip())

    # Fallbacks help with models where the NMI has also been copied into name/mRID.
    for attr in ("name", "mrid"):
        value = getattr(usage_point, attr, None)
        if value:
            candidates.add(str(value).strip())

    return candidates


def _prefer_consumer(candidate: EnergyConsumer, existing: EnergyConsumer) -> bool:
    if existing.location is None and candidate.location is not None:
        return True
    return False


def _build_feature_collection(points: List[MatchedLoopImpedancePoint]) -> FeatureCollection:
    features: List[Feature] = []
    for point in points:
        geometry = to_geojson_geometry(point.energy_consumer.location)
        if geometry is None:
            continue
        properties = {
            "type": "usage_point",
            "nmi": point.nmi,
            "name": point.energy_consumer.name or point.nmi,
            "energy_consumer_mrid": point.energy_consumer.mrid,
            "csv_row_count": point.source_row_count,
            "network_match_count": point.network_match_count,
            "r_gis": round(point.r_gis, 6),
            "r_est": round(point.r_est, 6),
            "r_error": round(point.r_error, 6),
            "r_gis_label": _format_ohm(point.r_gis),
            "r_est_label": _format_ohm(point.r_est),
            "r_error_label": _format_error(point.r_error),
        }
        features.append(Feature(point.nmi, geometry, properties))
    return FeatureCollection(features)


def _format_ohm(value: float) -> str:
    return f"{value:.3f}Ω"


def _format_error(value: float) -> str:
    return f"{value * 100.0:.1f}%"


def to_geojson_geometry(location: Location) -> Union[Geometry, None]:
    points = list(location.points) if location is not None else []
    if len(points) == 1:
        return Point((points[0].x_position, points[0].y_position))
    return None


if __name__ == "__main__":
    asyncio.run(main())
