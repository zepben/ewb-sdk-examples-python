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
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import aiohttp
from geojson import Feature, FeatureCollection
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import GeoJsonOverlay, Result, Study
from zepben.ewb import AcLineSegment, ConductingEquipment, PowerTransformer

try:
    from zepben.examples.studies.hosting_capacity_examples.common import (
        bucket_from_thresholds,
        build_feeder_trace_context,
        canonical_mz_type,
        catalog_from_slice_option_snapshot,
        combine_feeder_scope,
        compute_percentiles,
        create_postgres_engine,
        feeders_for_year_from_snapshot,
        fetch_metric_rows,
        fetch_prefixed_feeders,
        fetch_slice_option_snapshot,
        is_trace_source_mz_type,
        load_db_settings,
        load_ewb_settings,
        map_zone_heads_to_assets,
        print_slice_option_catalog,
        prompt_select_from_values,
        resolve_slice_selection,
        resolve_zone_feeders,
        should_apply_transformer_trace_fallback,
        season_time_of_day_pairs_from_snapshot,
        split_csv_values,
        to_equipment_geometry,
    )
except ModuleNotFoundError:
    from common import (  # type: ignore
        bucket_from_thresholds,
        build_feeder_trace_context,
        canonical_mz_type,
        catalog_from_slice_option_snapshot,
        combine_feeder_scope,
        compute_percentiles,
        create_postgres_engine,
        feeders_for_year_from_snapshot,
        fetch_metric_rows,
        fetch_prefixed_feeders,
        fetch_slice_option_snapshot,
        is_trace_source_mz_type,
        load_db_settings,
        load_ewb_settings,
        map_zone_heads_to_assets,
        print_slice_option_catalog,
        prompt_select_from_values,
        resolve_slice_selection,
        resolve_zone_feeders,
        should_apply_transformer_trace_fallback,
        season_time_of_day_pairs_from_snapshot,
        split_csv_values,
        to_equipment_geometry,
    )


STYLE_PATH = Path(__file__).resolve().parent / "style_hcm_asset_metrics.json"

METRIC_SPECS: List[Tuple[str, str, str]] = [
    ("peak_import", "Peak Import", "hc-peak-import"),
    ("peak_export", "Peak Export", "hc-peak-export"),
    ("import_utilisation", "Import Utilisation", "hc-import-utilisation"),
    ("export_utilisation", "Export Utilisation", "hc-export-utilisation"),
    (
        "load_exceeding_normal_thermal_voltage_kwh",
        "Load Exceeding Normal Thermal + Voltage (kWh)",
        "hc-load-thermal-voltage",
    ),
    (
        "gen_exceeding_normal_thermal_voltage_kwh",
        "Generation Exceeding Normal Thermal + Voltage (kWh)",
        "hc-gen-thermal-voltage",
    ),
]

BUCKET_LABELS = {
    0: "Very Low",
    1: "Low",
    2: "Medium",
    3: "High",
    4: "Very High",
}


def summarise_zone_names(zone_names: Set[str], limit: int = 4) -> str:
    ordered = sorted(name for name in zone_names if name)
    if not ordered:
        return ""
    if len(ordered) <= limit:
        return ", ".join(ordered)
    return ", ".join(ordered[:limit]) + f" (+{len(ordered) - limit} more)"


def _as_positive_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _transformer_rating_kva(eq: ConductingEquipment) -> Optional[float]:
    if not isinstance(eq, PowerTransformer):
        return None
    ratings: List[float] = []
    for end in getattr(eq, "ends", []) or []:
        rated_s = getattr(end, "rated_s", None)
        if rated_s is not None:
            ratings.append(float(rated_s) / 1000.0)
            continue
        for rating in getattr(end, "s_ratings", []) or []:
            rated_s = getattr(rating, "rated_s", None)
            if rated_s is not None:
                ratings.append(float(rated_s) / 1000.0)
    return max(ratings) if ratings else None


def _line_rating_amp(line: AcLineSegment) -> Optional[float]:
    direct = _as_positive_float(getattr(line, "rated_current", None))
    if direct is not None:
        return direct

    asset_info = getattr(line, "asset_info", None)
    if asset_info is not None:
        info_rating = _as_positive_float(getattr(asset_info, "rated_current", None))
        if info_rating is not None:
            return info_rating

    for limits_attr in ("normal_current_limits", "current_limits", "operational_limits"):
        limits = getattr(line, limits_attr, None)
        if not limits:
            continue
        values: List[float] = []
        for limit in limits:
            for field in ("value", "normal_value", "current_limit", "rated_current", "limit_value"):
                rating = _as_positive_float(getattr(limit, field, None))
                if rating is not None:
                    values.append(rating)
        if values:
            return max(values)
    return None


def _phase_letters_from_phase_code(phase_obj) -> Set[str]:
    if phase_obj is None:
        return set()
    candidates = [str(phase_obj).upper()]
    try:
        as_code = phase_obj.as_phase_code() if hasattr(phase_obj, "as_phase_code") else None
        if as_code is not None:
            candidates.append(str(as_code).upper())
    except Exception:
        pass

    letters: Set[str] = set()
    for text in candidates:
        token = text.split(".")[-1]
        for ch in token:
            if ch in {"A", "B", "C"}:
                letters.add(ch)
    return letters


def _line_phase_count(line: AcLineSegment) -> int:
    letters: Set[str] = set()
    for terminal in getattr(line, "terminals", []) or []:
        letters.update(_phase_letters_from_phase_code(getattr(terminal, "phases", None)))
        letters.update(_phase_letters_from_phase_code(getattr(terminal, "normal_phases", None)))

    if letters:
        return len(letters)
    return 3


def _first_zone_head_line_mrid(context, head_mrid: str) -> Optional[str]:
    equipment = context.equipment_by_mrid.get(head_mrid)
    if isinstance(equipment, AcLineSegment):
        return head_mrid

    visited: Set[str] = set()
    queue: List[str] = [head_mrid]
    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        for downstream_mrid in sorted(context.downstream_edges_by_from.get(current, set())):
            downstream_eq = context.equipment_by_mrid.get(downstream_mrid)
            if isinstance(downstream_eq, AcLineSegment):
                return downstream_mrid

            edge = (current, downstream_mrid)
            lines = sorted(context.traversed_lines_by_edge.get(edge, set()))
            if lines:
                return lines[0]
            if downstream_mrid not in visited:
                queue.append(downstream_mrid)
    return None


def _zone_rating(context, head_mrid: str) -> Tuple[Optional[float], Optional[str], Optional[str], Optional[int]]:
    equipment = context.equipment_by_mrid.get(head_mrid)
    if equipment is None:
        return None, None, None, None

    tx_rating = _transformer_rating_kva(equipment)
    if tx_rating is not None:
        return tx_rating, "kVA", head_mrid, None

    line_mrid = _first_zone_head_line_mrid(context, head_mrid)
    if line_mrid is None:
        return None, None, None, None
    line_eq = context.equipment_by_mrid.get(line_mrid)
    if not isinstance(line_eq, AcLineSegment):
        return None, None, None, None

    line_rating = _line_rating_amp(line_eq)
    phase_count = _line_phase_count(line_eq)
    if line_rating is None:
        return None, None, line_mrid, phase_count
    return line_rating, "A", line_mrid, phase_count


def _utilisation_value(
    metric_key: str,
    peak_import_kw: Optional[float],
    peak_export_kw: Optional[float],
    rating_value: Optional[float],
    rating_unit: Optional[str],
    v_base: Optional[float],
    phase_count: Optional[int],
) -> Optional[float]:
    if rating_value is None or rating_unit is None:
        return None

    if metric_key == "import_utilisation":
        source = peak_import_kw
        if source is None:
            return None
        magnitude = abs(float(source))
    elif metric_key == "export_utilisation":
        source = peak_export_kw
        if source is None:
            return None
        magnitude = abs(float(source))
    else:
        return None

    if rating_unit == "kVA":
        if rating_value <= 0:
            return None
        return (magnitude / rating_value) * 100.0

    if rating_unit == "A":
        phase_n = phase_count or 3
        if phase_n <= 0:
            phase_n = 3
        if rating_value <= 0 or v_base is None or v_base <= 0:
            return None
        # Line rating is per phase. DB peaks are total zone flow, so split
        # across available phases on the zone-head line.
        current_a = (magnitude * 1000.0 * math.sqrt(3.0)) / (phase_n * float(v_base))
        return (current_a / rating_value) * 100.0

    return None


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a Hosting Capacity asset metrics study using DB results propagated "
            "from measurement-zone heads until the next downstream measurement zone."
        )
    )
    parser.add_argument("--work-package-id", required=True, help="Work package ID.")
    parser.add_argument("--year", type=int, help="Result year (calendar year).")
    parser.add_argument("--zones", default="", help="Comma-separated zone codes.")
    parser.add_argument(
        "--feeder-prefixes",
        default="",
        help="Comma-separated feeder prefixes (matched against DB feeder column).",
    )
    parser.add_argument("--scenario", default="", help="Optional scenario override.")
    parser.add_argument(
        "--timestamp",
        default="",
        help="Optional timestamp override (ISO-8601, e.g. 2025-06-30T14:30:00+00:00).",
    )
    parser.add_argument("--season", default="all", help="Season filter (default: all).")
    parser.add_argument("--time-of-day", default="all", help="Time-of-day filter (default: all).")
    parser.add_argument("--env-file", default=".env", help="Path to .env with EWB_* and INPUT_DB_* keys.")
    parser.add_argument("--name", default="Hosting Capacity Asset Metrics", help="Study name.")
    parser.add_argument(
        "--list-options",
        action="store_true",
        help="List selectable year/scenario/timestamp/season/time-of-day/feeders for the work package and exit.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for missing slice selections (year/scenario/timestamp).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Generate results but do not upload.")
    return parser.parse_args(argv)


async def main(argv: Sequence[str]) -> None:
    args = parse_args(argv)

    zone_codes = split_csv_values(args.zones)
    feeder_prefixes = split_csv_values(args.feeder_prefixes)

    ewb_settings = load_ewb_settings(args.env_file)
    db_settings = load_db_settings(args.env_file)
    styles = json.load(open(STYLE_PATH, "r"))

    engine = create_postgres_engine(db_settings)

    try:
        zone_feeders = await resolve_zone_feeders(ewb_settings, zone_codes) if zone_codes else set()
        prefix_feeders = (
            fetch_prefixed_feeders(
                engine=engine,
                work_package_id=args.work_package_id,
                year=args.year,
                feeder_prefixes=feeder_prefixes,
            )
            if feeder_prefixes
            else set()
        )
        if feeder_prefixes and not prefix_feeders:
            print(
                "No feeders matched --feeder-prefixes for the current filter "
                f"(year={args.year if args.year is not None else 'ALL'})."
            )

        selected_feeders: Set[str] = set()
        if zone_feeders or prefix_feeders:
            selected_feeders = combine_feeder_scope(zone_feeders=zone_feeders, prefix_feeders=prefix_feeders)

        scenario_arg = args.scenario.strip() or None
        option_snapshot = None

        if args.list_options or args.interactive or args.year is None:
            option_snapshot = fetch_slice_option_snapshot(
                engine=engine,
                work_package_id=args.work_package_id,
                feeders=sorted(selected_feeders),
            )
            catalog = catalog_from_slice_option_snapshot(
                snapshot=option_snapshot,
                year=args.year,
                scenario=scenario_arg,
            )
            print_slice_option_catalog(
                catalog=catalog,
                work_package_id=args.work_package_id,
                selected_year=args.year,
                selected_scenario=scenario_arg,
                selected_feeders=sorted(selected_feeders),
            )

            if args.list_options and not args.interactive:
                return

            if args.year is None and not args.interactive:
                print("No year selected. Re-run with --year or use --interactive.")
                return

        resolved_year = args.year
        if resolved_year is None and args.interactive:
            if option_snapshot is None:
                option_snapshot = fetch_slice_option_snapshot(
                    engine=engine,
                    work_package_id=args.work_package_id,
                    feeders=sorted(selected_feeders),
                )
            unfiltered_catalog = catalog_from_slice_option_snapshot(snapshot=option_snapshot)
            selected_year_value = prompt_select_from_values(
                label="year",
                values=[str(value) for value in unfiltered_catalog.years],
            )
            resolved_year = int(selected_year_value)

        if resolved_year is None:
            raise ValueError("A year is required. Use --year or --interactive.")

        if not zone_codes and not feeder_prefixes:
            raise ValueError("At least one scope is required: --zones and/or --feeder-prefixes")

        if not selected_feeders:
            selected_feeders = combine_feeder_scope(zone_feeders=zone_feeders, prefix_feeders=prefix_feeders)

        if option_snapshot is None:
            option_snapshot = fetch_slice_option_snapshot(
                engine=engine,
                work_package_id=args.work_package_id,
                feeders=sorted(selected_feeders),
            )
        year_feeders = feeders_for_year_from_snapshot(option_snapshot, resolved_year)
        if not year_feeders:
            raise ValueError(
                f"No feeders in scope have rows for year {resolved_year}. "
                "Use --list-options to inspect available years."
            )
        selected_feeders = selected_feeders.intersection(year_feeders)
        if not selected_feeders:
            raise ValueError(
                f"No feeders matched the requested scope for year {resolved_year}."
            )

        resolved_scenario = scenario_arg
        resolved_timestamp = args.timestamp.strip() or None
        filtered_catalog = catalog_from_slice_option_snapshot(
            snapshot=option_snapshot,
            year=resolved_year,
            scenario=resolved_scenario,
        )
        if resolved_scenario is None:
            if not filtered_catalog.scenarios:
                raise ValueError("No scenarios available for selected year/scope.")
            if args.interactive:
                resolved_scenario = prompt_select_from_values(
                    label="scenario",
                    values=list(filtered_catalog.scenarios),
                )
            else:
                resolved_scenario = filtered_catalog.scenarios[0]

        filtered_catalog = catalog_from_slice_option_snapshot(
            snapshot=option_snapshot,
            year=resolved_year,
            scenario=resolved_scenario,
        )
        if resolved_timestamp is None:
            if not filtered_catalog.timestamps:
                raise ValueError("No timestamps available for selected year/scenario/scope.")
            if resolved_scenario is None:
                raise ValueError("Could not resolve scenario.")
            if args.interactive:
                resolved_timestamp = prompt_select_from_values(
                    label="timestamp",
                    values=[value.isoformat() for value in filtered_catalog.timestamps],
                )
            else:
                resolved_timestamp = filtered_catalog.timestamps[0].isoformat()

        resolved_season = args.season.strip() or "all"
        resolved_time_of_day = args.time_of_day.strip() or "all"

        selection = resolve_slice_selection(
            engine=engine,
            work_package_id=args.work_package_id,
            year=resolved_year,
            feeders=sorted(selected_feeders),
            scenario=resolved_scenario,
            timestamp=resolved_timestamp,
            season=resolved_season,
            time_of_day=resolved_time_of_day,
        )

        season_time_pairs = season_time_of_day_pairs_from_snapshot(
            snapshot=option_snapshot,
            year=selection.year,
            scenario=selection.scenario,
            timestamp=selection.timestamp,
        )
        if not season_time_pairs:
            raise RuntimeError(
                "No season/time_of_day rows available for the resolved work package/scenario/timestamp/feeders."
            )

        available_seasons = sorted({season for season, _ in season_time_pairs})
        if selection.season not in available_seasons:
            if args.interactive:
                season_default = "yearly" if "yearly" in available_seasons else available_seasons[0]
                selected_season = prompt_select_from_values(
                    label="season",
                    values=available_seasons,
                    default=season_default,
                )
                selection = resolve_slice_selection(
                    engine=engine,
                    work_package_id=args.work_package_id,
                    year=resolved_year,
                    feeders=sorted(selected_feeders),
                    scenario=selection.scenario,
                    timestamp=selection.timestamp.isoformat(),
                    season=selected_season,
                    time_of_day=selection.time_of_day,
                )
            else:
                raise ValueError(
                    f"Season '{selection.season}' is not valid for the resolved slice. "
                    f"Available seasons: {', '.join(available_seasons)}"
                )

        available_tods = sorted({tod for season, tod in season_time_pairs if season == selection.season})
        if selection.time_of_day not in available_tods:
            if args.interactive:
                tod_default = "all" if "all" in available_tods else available_tods[0]
                selected_tod = prompt_select_from_values(
                    label="time_of_day",
                    values=available_tods,
                    default=tod_default,
                )
                selection = resolve_slice_selection(
                    engine=engine,
                    work_package_id=args.work_package_id,
                    year=resolved_year,
                    feeders=sorted(selected_feeders),
                    scenario=selection.scenario,
                    timestamp=selection.timestamp.isoformat(),
                    season=selection.season,
                    time_of_day=selected_tod,
                )
            else:
                raise ValueError(
                    f"time_of_day '{selection.time_of_day}' is not valid for season '{selection.season}'. "
                    f"Available time_of_day values: {', '.join(available_tods)}"
                )

        print(
            "Resolved slice: "
            f"scenario={selection.scenario}, "
            f"timestamp={selection.timestamp.isoformat()}, "
            f"season={selection.season}, time_of_day={selection.time_of_day}, "
            f"feeders={len(selection.feeders)}"
        )

        rows = fetch_metric_rows(engine, selection)
        if not rows:
            raise RuntimeError("No rows returned for the resolved slice.")

        print(f"Fetched {len(rows)} DB rows.")

        rows_by_feeder: Dict[str, List] = defaultdict(list)
        for row in rows:
            rows_by_feeder[row.feeder].append(row)

        segment_geometries: Dict[str, object] = {}
        metric_segment_values: Dict[str, Dict[str, List[float]]] = {
            metric_key: defaultdict(list) for metric_key, _, _ in METRIC_SPECS
        }
        metric_segment_zones: Dict[str, Dict[str, Set[str]]] = {
            metric_key: defaultdict(set) for metric_key, _, _ in METRIC_SPECS
        }
        metric_segment_rating: Dict[str, Dict[str, Tuple[Optional[float], Optional[str], Optional[int]]]] = {
            metric_key: {} for metric_key, _, _ in METRIC_SPECS
        }

        missing_head_total = 0
        for feeder_mrid in sorted(rows_by_feeder.keys()):
            feeder_rows = rows_by_feeder[feeder_mrid]
            print(f"Tracing feeder {feeder_mrid} ({len(feeder_rows)} zone rows)")
            transformer_trace_fallback = should_apply_transformer_trace_fallback(
                [row.mz_type for row in feeder_rows]
            )
            if transformer_trace_fallback:
                print(
                    "  - transformer trace fallback enabled "
                    "(transformer/feeder-head-only measurement zones)."
                )

            context = await build_feeder_trace_context(ewb_settings, feeder_mrid)
            head_ids = {row.conducting_equipment_mrid for row in feeder_rows}
            head_to_assets, missing_heads = await map_zone_heads_to_assets(context, head_ids)

            if missing_heads:
                missing_head_total += len(missing_heads)
                print(f"  - missing zone heads in network model: {len(missing_heads)}")

            zone_rating_cache: Dict[str, Tuple[Optional[float], Optional[str], Optional[str], Optional[int]]] = {}
            trace_source_rows = 0
            point_source_rows = 0
            for row in feeder_rows:
                canonical_mz = canonical_mz_type(row.mz_type)
                use_trace_source = is_trace_source_mz_type(row.mz_type) or (
                    transformer_trace_fallback and canonical_mz == "TRANSFORMER"
                )

                if use_trace_source:
                    trace_source_rows += 1
                    mapped_assets = head_to_assets.get(row.conducting_equipment_mrid)
                else:
                    point_source_rows += 1
                    mapped_assets = {row.conducting_equipment_mrid}
                if not mapped_assets:
                    continue

                if row.conducting_equipment_mrid not in zone_rating_cache:
                    zone_rating_cache[row.conducting_equipment_mrid] = _zone_rating(context, row.conducting_equipment_mrid)
                zone_rating_value, zone_rating_unit, _, zone_phase_count = zone_rating_cache[row.conducting_equipment_mrid]

                row_metric_values: Dict[str, Optional[float]] = {
                    "peak_import": row.peak_import,
                    "peak_export": row.peak_export,
                    "load_exceeding_normal_thermal_voltage_kwh": row.load_exceeding_normal_thermal_voltage_kwh,
                    "gen_exceeding_normal_thermal_voltage_kwh": row.gen_exceeding_normal_thermal_voltage_kwh,
                    "import_utilisation": _utilisation_value(
                        "import_utilisation",
                        row.peak_import,
                        row.peak_export,
                        zone_rating_value,
                        zone_rating_unit,
                        row.v_base,
                        zone_phase_count,
                    ),
                    "export_utilisation": _utilisation_value(
                        "export_utilisation",
                        row.peak_import,
                        row.peak_export,
                        zone_rating_value,
                        zone_rating_unit,
                        row.v_base,
                        zone_phase_count,
                    ),
                }

                for asset_mrid in (mapped_assets or set()):
                    equipment = context.equipment_by_mrid.get(asset_mrid)
                    if equipment is None:
                        continue
                    geom_kind, geometry = to_equipment_geometry(equipment)
                    if geometry is None or geom_kind != "line":
                        continue

                    segment_key = f"asset:{asset_mrid}"
                    segment_geometries.setdefault(segment_key, geometry)
                    for metric_key, _, _ in METRIC_SPECS:
                        value = row_metric_values.get(metric_key)
                        if value is None:
                            continue
                        metric_segment_values[metric_key][segment_key].append(float(value))
                        metric_segment_zones[metric_key][segment_key].add(row.measurement_zone_name)
                        if metric_key in {"import_utilisation", "export_utilisation"}:
                            metric_segment_rating[metric_key][segment_key] = (zone_rating_value, zone_rating_unit, zone_phase_count)

            print(
                "  - zone source rows: "
                f"trace={trace_source_rows}, point={point_source_rows}"
            )

        print(f"Total mapped line segments: {len(segment_geometries)}")
        if missing_head_total:
            print(f"Total missing zone heads: {missing_head_total}")

        results: List[Result] = []
        for metric_key, metric_name, style_prefix in METRIC_SPECS:
            by_segment = metric_segment_values[metric_key]
            aggregated = {
                segment_key: max(values)
                for segment_key, values in by_segment.items()
                if values
            }

            thresholds = compute_percentiles(list(aggregated.values()), [0.50, 0.75, 0.90, 0.99])
            features: List[Feature] = []

            for segment_key, value in aggregated.items():
                geometry = segment_geometries.get(segment_key)
                if geometry is None:
                    continue

                bucket = bucket_from_thresholds(value, thresholds)
                zone_names = metric_segment_zones[metric_key].get(segment_key, set())
                rating_value, rating_unit, zone_phase_count = metric_segment_rating.get(metric_key, {}).get(segment_key, (None, None, None))
                rating_label = ""
                if rating_value is not None and rating_unit is not None:
                    if rating_unit == "A":
                        phase_n = zone_phase_count or 3
                        rating_label = f"{rating_value:.2f} A/ph x{phase_n}"
                    else:
                        rating_label = f"{rating_value:.2f} {rating_unit}"

                value_label = f"{value:.2f}"
                if metric_key in {"import_utilisation", "export_utilisation"}:
                    value_label = f"{value:.1f}%"
                    if rating_label:
                        value_label = f"{value_label} | {rating_label}"
                features.append(
                    Feature(
                        id=f"{metric_key}:{segment_key}",
                        geometry=geometry,
                        properties={
                            "metric_key": metric_key,
                            "metric_value": round(value, 6),
                            "bucket": bucket,
                            "bucket_label": BUCKET_LABELS[bucket],
                            "geom_kind": "line",
                            "segment_key": segment_key,
                            "value_label": value_label,
                            "zone_rating_value": (round(rating_value, 6) if rating_value is not None else None),
                            "zone_rating_unit": rating_unit,
                            "zone_phase_count": zone_phase_count,
                            "zone_rating_label": rating_label,
                            "measurement_zone_count": len(zone_names),
                            "measurement_zone_names": summarise_zone_names(zone_names),
                            "q50": round(thresholds[0], 6),
                            "q75": round(thresholds[1], 6),
                            "q90": round(thresholds[2], 6),
                            "q99": round(thresholds[3], 6),
                        },
                    )
                )

            print(f"  - {metric_name}: {len(features)} line feature(s)")

            results.append(
                Result(
                    name=metric_name,
                    geo_json_overlay=GeoJsonOverlay(
                        data=FeatureCollection(features),
                        styles=[f"{style_prefix}-line", f"{style_prefix}-label"],
                    ),
                )
            )

        study = Study(
            name=args.name,
            description=(
                "Hosting Capacity metric layers sourced from public.network_performance_metrics_enhanced, "
                "propagated from each measurement-zone head until the next downstream measurement-zone head."
            ),
            tags=[
                "hosting_capacity",
                "network_performance_metrics",
                f"work_package_{args.work_package_id}",
                f"scenario_{selection.scenario}",
                f"season_{selection.season}",
                f"time_of_day_{selection.time_of_day}",
            ],
            results=results,
            styles=styles,
        )

        if args.dry_run:
            print("Dry-run enabled. Study was not uploaded.")
            return

        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
        eas_client = EasClient(
            host=ewb_settings.host,
            port=ewb_settings.rpc_port,
            protocol="https",
            access_token=ewb_settings.access_token,
            session=session,
        )
        try:
            print("Uploading study...")
            response = await eas_client.async_upload_study(study)
            print(f"Study upload response: {response}")
        finally:
            await eas_client.aclose()
    finally:
        engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
