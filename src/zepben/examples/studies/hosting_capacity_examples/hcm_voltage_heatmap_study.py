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
from typing import Dict, List, Sequence

import aiohttp
from geojson import Feature, FeatureCollection
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import GeoJsonOverlay, Result, Study

try:
    from zepben.examples.studies.hosting_capacity_examples.common import (
        build_feeder_trace_context,
        canonical_mz_type,
        catalog_from_slice_option_snapshot,
        combine_feeder_scope,
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
        build_feeder_trace_context,
        canonical_mz_type,
        catalog_from_slice_option_snapshot,
        combine_feeder_scope,
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


STYLE_PATH = Path(__file__).resolve().parent / "style_hcm_voltage_heatmap.json"
V1_RED_LOW_LIMIT = 216.0
V1_GREEN_LOW = 225.0
V1_GREEN_HIGH = 240.0
V99_RED_HIGH_LIMIT = 235.0
V99_GREEN_LOW = 225.0


def voltage_label(voltage_lg: float, pu: float | None) -> str:
    if pu is None:
        return f"{voltage_lg:.1f} V"
    return f"{voltage_lg:.1f} V ({pu:.3f} pu)"


def v1_bucket(voltage_lg: float) -> int:
    if voltage_lg < V1_RED_LOW_LIMIT or voltage_lg > V1_GREEN_HIGH:
        return 0  # red: outside technical range
    if voltage_lg < V1_GREEN_LOW:
        return 1  # orange: approaching lower limit
    return 2  # green: normal range


def v99_bucket(voltage_lg: float) -> int:
    if voltage_lg > V99_RED_HIGH_LIMIT or voltage_lg < V1_RED_LOW_LIMIT:
        return 0  # red: outside technical range
    if voltage_lg < V99_GREEN_LOW:
        return 1  # orange: approaching lower side
    return 2  # green: normal range for this metric


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a Hosting Capacity voltage heatmap study using V1 and V99 magnitudes "
            "converted to line-ground voltage from v_base."
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
    parser.add_argument("--name", default="Hosting Capacity Voltage Heatmaps", help="Study name.")
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

        selected_feeders = set()
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
        v1_by_asset: Dict[str, float] = {}
        v1_base_by_asset: Dict[str, float] = {}
        v1_zone_name_by_asset: Dict[str, str] = {}
        v1_mz_type_by_asset: Dict[str, str] = {}
        v99_by_asset: Dict[str, float] = {}
        v99_base_by_asset: Dict[str, float] = {}
        v99_zone_name_by_asset: Dict[str, str] = {}
        v99_mz_type_by_asset: Dict[str, str] = {}

        missing_head_total = 0
        skipped_no_vbase = 0

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

            trace_source_rows = 0
            point_source_rows = 0
            for row in feeder_rows:
                if row.v_base is None or row.v_base <= 0:
                    skipped_no_vbase += 1
                    continue

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

                v_base_lg = float(row.v_base) / math.sqrt(3.0)

                v1_lg = None
                if row.v1_avg_section_voltage is not None:
                    v1_lg = float(row.v1_avg_section_voltage) * v_base_lg

                v99_lg = None
                if row.v99_avg_section_voltage is not None:
                    v99_lg = float(row.v99_avg_section_voltage) * v_base_lg

                for asset_mrid in (mapped_assets or set()):
                    equipment = context.equipment_by_mrid.get(asset_mrid)
                    if equipment is None:
                        continue

                    geom_kind, geometry = to_equipment_geometry(equipment)
                    if geometry is None or geom_kind != "line":
                        continue
                    segment_key = f"asset:{asset_mrid}"
                    segment_geometries[segment_key] = geometry

                    if v1_lg is not None:
                        existing_v1 = v1_by_asset.get(segment_key)
                        if existing_v1 is None or v1_lg < existing_v1:
                            v1_by_asset[segment_key] = v1_lg
                            v1_base_by_asset[segment_key] = v_base_lg
                            v1_zone_name_by_asset[segment_key] = row.measurement_zone_name
                            v1_mz_type_by_asset[segment_key] = row.mz_type

                    if v99_lg is not None:
                        existing_v99 = v99_by_asset.get(segment_key)
                        if existing_v99 is None or v99_lg > existing_v99:
                            v99_by_asset[segment_key] = v99_lg
                            v99_base_by_asset[segment_key] = v_base_lg
                            v99_zone_name_by_asset[segment_key] = row.measurement_zone_name
                            v99_mz_type_by_asset[segment_key] = row.mz_type

            print(
                "  - zone source rows: "
                f"trace={trace_source_rows}, point={point_source_rows}"
            )

        print(f"Mapped line segments: {len(segment_geometries)}")
        if missing_head_total:
            print(f"Total missing zone heads: {missing_head_total}")
        if skipped_no_vbase:
            print(f"Rows skipped due to missing/invalid v_base: {skipped_no_vbase}")

        v1_features: List[Feature] = []
        for asset_mrid, voltage_lg in v1_by_asset.items():
            geometry = segment_geometries.get(asset_mrid)
            if geometry is None:
                continue

            v_base_lg = v1_base_by_asset.get(asset_mrid, 0.0)
            pu = (voltage_lg / v_base_lg) if v_base_lg > 0 else None
            bucket = v1_bucket(voltage_lg)
            zone_name = v1_zone_name_by_asset.get(asset_mrid, "")

            v1_features.append(
                Feature(
                    id=f"v1:{asset_mrid}",
                    geometry=geometry,
                    properties={
                        "metric_key": "v1_avg_section_voltage",
                        "asset_mrid": asset_mrid,
                        "v_base_lg": round(v_base_lg, 6),
                        "voltage_lg_v": round(voltage_lg, 6),
                        "voltage_pu": (round(pu, 8) if pu is not None else None),
                        "value_label": voltage_label(voltage_lg, pu),
                        "bucket": bucket,
                        "bucket_label": ("Outside limit" if bucket == 0 else "Approaching limit" if bucket == 1 else "Normal range"),
                        "measurement_zone_name": zone_name,
                        "measurement_zone_type": v1_mz_type_by_asset.get(asset_mrid, ""),
                        "technical_limit_low_v": V1_RED_LOW_LIMIT,
                        "green_range_low_v": V1_GREEN_LOW,
                        "green_range_high_v": V1_GREEN_HIGH,
                        "conversion": "v1_avg_section_voltage * (v_base/sqrt(3))",
                    },
                )
            )

        v99_features: List[Feature] = []
        for asset_mrid, voltage_lg in v99_by_asset.items():
            geometry = segment_geometries.get(asset_mrid)
            if geometry is None:
                continue

            v_base_lg = v99_base_by_asset.get(asset_mrid, 0.0)
            pu = (voltage_lg / v_base_lg) if v_base_lg > 0 else None
            bucket = v99_bucket(voltage_lg)
            zone_name = v99_zone_name_by_asset.get(asset_mrid, "")

            v99_features.append(
                Feature(
                    id=f"v99:{asset_mrid}",
                    geometry=geometry,
                    properties={
                        "metric_key": "v99_avg_section_voltage",
                        "asset_mrid": asset_mrid,
                        "v_base_lg": round(v_base_lg, 6),
                        "voltage_lg_v": round(voltage_lg, 6),
                        "voltage_pu": (round(pu, 8) if pu is not None else None),
                        "value_label": voltage_label(voltage_lg, pu),
                        "bucket": bucket,
                        "bucket_label": ("Outside limit" if bucket == 0 else "Approaching limit" if bucket == 1 else "Normal range"),
                        "measurement_zone_name": zone_name,
                        "measurement_zone_type": v99_mz_type_by_asset.get(asset_mrid, ""),
                        "technical_limit_high_v": V99_RED_HIGH_LIMIT,
                        "green_range_low_v": V99_GREEN_LOW,
                        "green_range_high_v": V99_RED_HIGH_LIMIT,
                        "conversion": "v99_avg_section_voltage * (v_base/sqrt(3))",
                    },
                )
            )

        print(f"  - V1 heatmap features: {len(v1_features)}")
        print(f"  - V99 heatmap features: {len(v99_features)}")

        results = [
            Result(
                name="V1 Avg Section Voltage (Line-Ground)",
                geo_json_overlay=GeoJsonOverlay(
                    data=FeatureCollection(v1_features),
                    styles=["hc-v1-line", "hc-v1-label"],
                ),
            ),
            Result(
                name="V99 Avg Section Voltage (Line-Ground)",
                geo_json_overlay=GeoJsonOverlay(
                    data=FeatureCollection(v99_features),
                    styles=["hc-v99-line", "hc-v99-label"],
                ),
            ),
        ]

        study = Study(
            name=args.name,
            description=(
                "Hosting Capacity voltage heatmaps derived from measurement-zone results and propagated "
                "to downstream assets until the next measurement-zone head. V1 and V99 are converted to "
                "line-ground voltage magnitudes: v*_avg_section_voltage * (v_base/sqrt(3))."
            ),
            tags=[
                "hosting_capacity",
                "network_performance_metrics",
                "voltage_heatmap",
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
