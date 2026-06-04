#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import DefaultDict, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from dotenv import dotenv_values
from geojson.geometry import LineString, Point
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine, URL
from zepben.examples.studies.study_utils import ca_filename_from_config
from zepben.ewb import (
    AcLineSegment,
    ConductingEquipment,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    NetworkTraceStep,
    PhaseCode,
    Tracing,
    connect_with_token,
    downstream,
)
from zepben.ewb.services.network.tracing.networktrace.operators.network_state_operators import NetworkStateOperators


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class EwbSettings:
    host: str
    rpc_port: int
    access_token: str
    ca_filename: Optional[str]
    timeout_seconds: int
    debug: bool
    skip_connection_test: bool


@dataclass(frozen=True)
class SliceSelection:
    work_package_id: str
    year: int
    scenario: str
    timestamp: datetime
    season: str
    time_of_day: str
    feeders: Tuple[str, ...]


@dataclass(frozen=True)
class DbMetricRow:
    work_package_id: str
    scenario: str
    timestamp: datetime
    feeder: str
    measurement_zone_name: str
    mz_type: str
    conducting_equipment_mrid: str
    season: str
    time_of_day: str
    v_base: Optional[float]
    peak_import: Optional[float]
    peak_export: Optional[float]
    load_exceeding_normal_thermal_voltage_kwh: Optional[float]
    gen_exceeding_normal_thermal_voltage_kwh: Optional[float]
    v1_avg_section_voltage: Optional[float]
    v99_avg_section_voltage: Optional[float]


@dataclass(frozen=True)
class SliceOptionCatalog:
    years: Tuple[int, ...]
    feeders: Tuple[str, ...]
    scenarios: Tuple[str, ...]
    timestamps: Tuple[datetime, ...]
    seasons: Tuple[str, ...]
    time_of_days: Tuple[str, ...]


@dataclass(frozen=True)
class SliceOptionRecord:
    year: int
    feeder: str
    scenario: str
    timestamp: datetime
    season: str
    time_of_day: str


@dataclass(frozen=True)
class SliceOptionSnapshot:
    records: Tuple[SliceOptionRecord, ...]


@dataclass
class FeederTraceContext:
    feeder_mrid: str
    equipment_by_mrid: Dict[str, ConductingEquipment]
    downstream_edges_by_from: Dict[str, Set[str]]
    traversed_lines_by_edge: Dict[Tuple[str, str], Set[str]]


POINT_SOURCE_MZ_TYPES: Set[str] = {"TRANSFORMER"}
TRACE_SOURCE_MZ_TOKENS: Tuple[str, ...] = ("FEEDER_HEAD", "FUSE", "SWITCH", "LINE")


def split_csv_values(raw: str) -> List[str]:
    if not raw:
        return []
    return [value.strip() for value in raw.split(",") if value and value.strip()]


def canonical_mz_type(mz_type: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", (mz_type or "").strip().upper()).strip("_")


def is_point_source_mz_type(mz_type: str) -> bool:
    return canonical_mz_type(mz_type) in POINT_SOURCE_MZ_TYPES


def is_trace_source_mz_type(mz_type: str) -> bool:
    canonical = canonical_mz_type(mz_type)
    if canonical in POINT_SOURCE_MZ_TYPES:
        return False
    if any(token in canonical for token in TRACE_SOURCE_MZ_TOKENS):
        return True
    # Default to trace for unknown types to avoid silently dropping zones.
    return True


def should_apply_transformer_trace_fallback(mz_types: Sequence[str]) -> bool:
    canonical = [canonical_mz_type(value) for value in mz_types if (value or "").strip()]
    if not canonical:
        return False

    has_transformer = any(value == "TRANSFORMER" for value in canonical)
    if not has_transformer:
        return False

    # Fallback is only for the "point-only transformer zones" case.
    allowed_point_only = {"TRANSFORMER", "FEEDER_HEAD"}
    if any(value not in allowed_point_only for value in canonical):
        return False

    # If explicit LV group heads exist, use normal trace classification.
    has_explicit_lv_trace_heads = any(
        any(token in value for token in ("FUSE", "SWITCH", "LINE"))
        for value in canonical
    )
    if has_explicit_lv_trace_heads:
        return False

    return True


def _load_env_values(env_file: str) -> Mapping[str, Optional[str]]:
    env_path = Path(env_file)
    return dotenv_values(str(env_path))


def _parse_bool(raw: Optional[str], default: bool = False) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_db_settings(env_file: str) -> DbSettings:
    env_values = _load_env_values(env_file)

    required = [
        "INPUT_DB_HOST",
        "INPUT_DB_PORT",
        "INPUT_DB_USER",
        "INPUT_DB_PASSWORD",
        "INPUT_DB_NAME",
    ]
    missing = [key for key in required if not (env_values.get(key) or "").strip()]
    if missing:
        raise ValueError(
            "Missing required database settings in .env: " + ", ".join(missing)
        )

    return DbSettings(
        host=str(env_values["INPUT_DB_HOST"]),
        port=int(str(env_values["INPUT_DB_PORT"])),
        user=str(env_values["INPUT_DB_USER"]),
        password=str(env_values["INPUT_DB_PASSWORD"]),
        database=str(env_values["INPUT_DB_NAME"]),
    )


def load_ewb_settings(env_file: str) -> EwbSettings:
    env_values = _load_env_values(env_file)

    required = [
        "EWB_HOST",
        "EWB_RPC_PORT",
        "EWB_ACCESS_TOKEN",
    ]
    missing = [key for key in required if not (env_values.get(key) or "").strip()]
    if missing:
        raise ValueError(
            "Missing required EWB settings in .env: " + ", ".join(missing)
        )

    ca_filename = (env_values.get("EWB_CA_FILENAME") or "").strip() or None
    timeout_raw = (env_values.get("EWB_TIMEOUT_SECONDS") or "").strip()
    timeout_seconds = int(timeout_raw) if timeout_raw else 30

    return EwbSettings(
        host=str(env_values["EWB_HOST"]),
        rpc_port=int(str(env_values["EWB_RPC_PORT"])),
        access_token=str(env_values["EWB_ACCESS_TOKEN"]),
        ca_filename=ca_filename,
        timeout_seconds=timeout_seconds,
        debug=_parse_bool(env_values.get("EWB_DEBUG"), default=False),
        skip_connection_test=_parse_bool(env_values.get("EWB_SKIP_CONNECTION_TEST"), default=False),
    )


def create_postgres_engine(settings: DbSettings) -> Engine:
    url = URL.create(
        drivername="postgresql+pg8000",
        username=settings.user,
        password=settings.password,
        host=settings.host,
        port=settings.port,
        database=settings.database,
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def _connect_rpc(ewb: EwbSettings):
    return connect_with_token(
        host=ewb.host,
        access_token=ewb.access_token,
        rpc_port=ewb.rpc_port,
        ca_filename=ewb.ca_filename or ca_filename_from_config({}),
        timeout_seconds=ewb.timeout_seconds,
        debug=ewb.debug,
        skip_connection_test=ewb.skip_connection_test,
    )


def _is_deadline_exceeded_error(error: object) -> bool:
    text = str(error or "").upper()
    return "DEADLINE_EXCEEDED" in text or "DEADLINE EXCEEDED" in text


def _year_bounds(year: int) -> Tuple[datetime, datetime]:
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    return start, end


def _parse_timestamp(value: str) -> datetime:
    normalised = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalised)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


async def resolve_zone_feeders(ewb: EwbSettings, zone_codes: Sequence[str]) -> Set[str]:
    if not zone_codes:
        return set()

    rpc_channel = _connect_rpc(ewb)
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()
    substations = hierarchy.value.substations

    feeders: Set[str] = set()
    missing: List[str] = []

    for zone in zone_codes:
        zone_data = substations.get(zone)
        if zone_data is None:
            missing.append(zone)
            continue
        for feeder in zone_data.feeders:
            feeders.add(str(feeder.mrid))

    if missing:
        available = ", ".join(sorted(substations.keys()))
        raise ValueError(f"Unknown zone code(s): {', '.join(missing)}. Available zones include: {available}")

    return feeders


def fetch_prefixed_feeders(
    engine: Engine,
    work_package_id: str,
    year: Optional[int],
    feeder_prefixes: Sequence[str],
) -> Set[str]:
    if not feeder_prefixes:
        return set()

    prefix_terms = []
    params: Dict[str, object] = {"work_package_id": work_package_id}

    for i, prefix in enumerate(feeder_prefixes):
        key = f"prefix_{i}"
        prefix_terms.append(f"feeder::text LIKE :{key}")
        params[key] = f"{prefix}%"

    year_clause = ""
    if year is not None:
        year_start, year_end = _year_bounds(year)
        params["year_start"] = year_start
        params["year_end"] = year_end
        year_clause = ' AND "timestamp" >= :year_start AND "timestamp" < :year_end'

    sql = f"""
        SELECT DISTINCT feeder::text AS feeder
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
          {year_clause}
          AND ({' OR '.join(prefix_terms)})
        ORDER BY feeder::text
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return {str(row["feeder"]) for row in rows if row.get("feeder") is not None}


def combine_feeder_scope(zone_feeders: Set[str], prefix_feeders: Set[str]) -> Set[str]:
    if zone_feeders and prefix_feeders:
        selected = zone_feeders.intersection(prefix_feeders)
    elif zone_feeders:
        selected = zone_feeders
    elif prefix_feeders:
        selected = prefix_feeders
    else:
        raise ValueError("At least one feeder scope is required: --zones and/or --feeder-prefixes")

    if not selected:
        raise ValueError("No feeders matched the requested scope intersection.")

    return selected


def _feeder_clause(feeders: Sequence[str]) -> Tuple[str, bool]:
    if not feeders:
        return "", False
    return " AND feeder::text IN :feeders", True


def _year_clause(year: Optional[int]) -> Tuple[str, Optional[datetime], Optional[datetime]]:
    if year is None:
        return "", None, None
    year_start, year_end = _year_bounds(year)
    return ' AND "timestamp" >= :year_start AND "timestamp" < :year_end', year_start, year_end


def _normalise_optional_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None


def _base_option_filters(
    work_package_id: str,
    year: Optional[int],
    scenario: Optional[str],
    feeders: Sequence[str],
) -> Tuple[str, Dict[str, object], bool]:
    feeder_clause, has_feeders = _feeder_clause(feeders)
    year_clause, year_start, year_end = _year_clause(year)
    scenario_value = _normalise_optional_text(scenario)
    scenario_clause = " AND scenario = :scenario" if scenario_value else ""

    where_clause = f"""
        WHERE work_package_id = :work_package_id
          {year_clause}
          {scenario_clause}
          {feeder_clause}
    """

    params: Dict[str, object] = {"work_package_id": work_package_id}
    if year_start is not None and year_end is not None:
        params["year_start"] = year_start
        params["year_end"] = year_end
    if scenario_value:
        params["scenario"] = scenario_value
    if has_feeders:
        params["feeders"] = list(feeders)

    return where_clause, params, has_feeders


def _select_distinct_text_values(
    engine: Engine,
    select_expression: str,
    where_clause: str,
    params: Dict[str, object],
    has_feeders: bool,
) -> List[str]:
    stmt = text(
        f"""
        SELECT DISTINCT {select_expression} AS value
        FROM public.network_performance_metrics_enhanced
        {where_clause}
        ORDER BY value
        """
    )
    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))
    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()
    return [str(row["value"]) for row in rows if row.get("value") is not None]


def _select_distinct_timestamps(
    engine: Engine,
    where_clause: str,
    params: Dict[str, object],
    has_feeders: bool,
) -> List[datetime]:
    stmt = text(
        f"""
        SELECT DISTINCT "timestamp" AS value
        FROM public.network_performance_metrics_enhanced
        {where_clause}
        ORDER BY value
        """
    )
    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    values: List[datetime] = []
    for row in rows:
        value = row.get("value")
        if value is None:
            continue
        if isinstance(value, datetime):
            values.append(value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc))
            continue
        values.append(_parse_timestamp(str(value)))
    return values


def fetch_slice_option_snapshot(
    engine: Engine,
    work_package_id: str,
    feeders: Sequence[str] = (),
) -> SliceOptionSnapshot:
    feeder_clause, has_feeders = _feeder_clause(feeders)
    stmt = text(
        f"""
        SELECT DISTINCT
            EXTRACT(YEAR FROM "timestamp" AT TIME ZONE 'UTC')::int AS year,
            feeder::text AS feeder,
            scenario,
            "timestamp" AS timestamp,
            season,
            time_of_day
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
          {feeder_clause}
        ORDER BY year, scenario, "timestamp", season, time_of_day, feeder::text
        """
    )
    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))

    params: Dict[str, object] = {"work_package_id": work_package_id}
    if has_feeders:
        params["feeders"] = list(feeders)

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    records: List[SliceOptionRecord] = []
    for row in rows:
        year = row.get("year")
        feeder = row.get("feeder")
        scenario = row.get("scenario")
        ts = row.get("timestamp")
        season = row.get("season")
        time_of_day = row.get("time_of_day")
        if year is None or feeder is None or scenario is None or ts is None or season is None or time_of_day is None:
            continue

        if isinstance(ts, datetime):
            timestamp = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        else:
            timestamp = _parse_timestamp(str(ts))

        records.append(
            SliceOptionRecord(
                year=int(year),
                feeder=str(feeder),
                scenario=str(scenario),
                timestamp=timestamp,
                season=str(season),
                time_of_day=str(time_of_day),
            )
        )

    return SliceOptionSnapshot(records=tuple(records))


def _filter_slice_option_records(
    records: Sequence[SliceOptionRecord],
    year: Optional[int] = None,
    scenario: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> List[SliceOptionRecord]:
    scenario_filter = _normalise_optional_text(scenario)
    filtered: List[SliceOptionRecord] = []
    for record in records:
        if year is not None and record.year != year:
            continue
        if scenario_filter is not None and record.scenario != scenario_filter:
            continue
        if timestamp is not None and record.timestamp != timestamp:
            continue
        filtered.append(record)
    return filtered


def catalog_from_slice_option_snapshot(
    snapshot: SliceOptionSnapshot,
    year: Optional[int] = None,
    scenario: Optional[str] = None,
    timestamp: Optional[datetime] = None,
) -> SliceOptionCatalog:
    filtered = _filter_slice_option_records(
        records=snapshot.records,
        year=year,
        scenario=scenario,
        timestamp=timestamp,
    )
    years = tuple(sorted({record.year for record in filtered}))
    feeders = tuple(sorted({record.feeder for record in filtered}))
    scenarios = tuple(sorted({record.scenario for record in filtered}))
    timestamps = tuple(sorted({record.timestamp for record in filtered}))
    seasons = tuple(sorted({record.season for record in filtered}))
    time_of_days = tuple(sorted({record.time_of_day for record in filtered}))
    return SliceOptionCatalog(
        years=years,
        feeders=feeders,
        scenarios=scenarios,
        timestamps=timestamps,
        seasons=seasons,
        time_of_days=time_of_days,
    )


def season_time_of_day_pairs_from_snapshot(
    snapshot: SliceOptionSnapshot,
    year: int,
    scenario: str,
    timestamp: datetime,
) -> List[Tuple[str, str]]:
    filtered = _filter_slice_option_records(
        records=snapshot.records,
        year=year,
        scenario=scenario,
        timestamp=timestamp,
    )
    pairs = sorted({(record.season, record.time_of_day) for record in filtered})
    return list(pairs)


def feeders_for_year_from_snapshot(snapshot: SliceOptionSnapshot, year: int) -> Set[str]:
    return {record.feeder for record in snapshot.records if record.year == year}


def fetch_slice_option_catalog(
    engine: Engine,
    work_package_id: str,
    year: Optional[int] = None,
    scenario: Optional[str] = None,
    feeders: Sequence[str] = (),
) -> SliceOptionCatalog:
    years_stmt = text(
        """
        SELECT DISTINCT EXTRACT(YEAR FROM "timestamp" AT TIME ZONE 'UTC')::int AS year
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
        ORDER BY year
        """
    )
    with engine.connect() as conn:
        year_rows = conn.execute(years_stmt, {"work_package_id": work_package_id}).mappings().all()
    years = tuple(int(row["year"]) for row in year_rows if row.get("year") is not None)

    where_clause, params, has_feeders = _base_option_filters(
        work_package_id=work_package_id,
        year=year,
        scenario=scenario,
        feeders=feeders,
    )

    feeders_values = tuple(
        _select_distinct_text_values(
            engine=engine,
            select_expression="feeder::text",
            where_clause=where_clause,
            params=params,
            has_feeders=has_feeders,
        )
    )
    scenarios = tuple(
        _select_distinct_text_values(
            engine=engine,
            select_expression="scenario",
            where_clause=where_clause,
            params=params,
            has_feeders=has_feeders,
        )
    )
    seasons = tuple(
        _select_distinct_text_values(
            engine=engine,
            select_expression="season",
            where_clause=where_clause,
            params=params,
            has_feeders=has_feeders,
        )
    )
    time_of_days = tuple(
        _select_distinct_text_values(
            engine=engine,
            select_expression="time_of_day",
            where_clause=where_clause,
            params=params,
            has_feeders=has_feeders,
        )
    )
    timestamps = tuple(
        _select_distinct_timestamps(
            engine=engine,
            where_clause=where_clause,
            params=params,
            has_feeders=has_feeders,
        )
    )

    return SliceOptionCatalog(
        years=years,
        feeders=feeders_values,
        scenarios=scenarios,
        timestamps=timestamps,
        seasons=seasons,
        time_of_days=time_of_days,
    )


def fetch_slice_season_time_of_day_pairs(
    engine: Engine,
    work_package_id: str,
    year: int,
    scenario: str,
    timestamp: datetime,
    feeders: Sequence[str],
) -> List[Tuple[str, str]]:
    year_start, year_end = _year_bounds(year)
    feeder_clause, has_feeders = _feeder_clause(feeders)

    stmt = text(
        f"""
        SELECT DISTINCT season, time_of_day
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
          AND scenario = :scenario
          AND "timestamp" = :timestamp
          AND "timestamp" >= :year_start
          AND "timestamp" < :year_end
          {feeder_clause}
        ORDER BY season, time_of_day
        """
    )
    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))

    params: Dict[str, object] = {
        "work_package_id": work_package_id,
        "scenario": scenario,
        "timestamp": timestamp,
        "year_start": year_start,
        "year_end": year_end,
    }
    if has_feeders:
        params["feeders"] = list(feeders)

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    pairs: List[Tuple[str, str]] = []
    for row in rows:
        season = row.get("season")
        time_of_day = row.get("time_of_day")
        if season is None or time_of_day is None:
            continue
        pairs.append((str(season), str(time_of_day)))
    return pairs


def print_slice_option_catalog(
    catalog: SliceOptionCatalog,
    work_package_id: str,
    selected_year: Optional[int],
    selected_scenario: Optional[str],
    selected_feeders: Sequence[str],
) -> None:
    print(f"Selectable options for work_package_id={work_package_id}")
    print(
        "Active filters: "
        f"year={selected_year if selected_year is not None else 'ALL'}, "
        f"scenario={selected_scenario.strip() if selected_scenario else 'ALL'}, "
        f"feeders={'ALL' if not selected_feeders else len(selected_feeders)}"
    )

    if catalog.years:
        print("  years: " + ", ".join(str(value) for value in catalog.years))
    else:
        print("  years: <none>")

    if catalog.scenarios:
        print("  scenarios: " + ", ".join(catalog.scenarios))
    else:
        print("  scenarios: <none>")

    if catalog.seasons:
        print("  seasons: " + ", ".join(catalog.seasons))
    else:
        print("  seasons: <none>")

    if catalog.time_of_days:
        print("  time_of_day: " + ", ".join(catalog.time_of_days))
    else:
        print("  time_of_day: <none>")

    if catalog.timestamps:
        print(f"  timestamps: {len(catalog.timestamps)} option(s)")
        preview = [value.isoformat() for value in catalog.timestamps[:20]]
        print("    " + ", ".join(preview))
        if len(catalog.timestamps) > 20:
            print("    ...")
    else:
        print("  timestamps: <none>")

    if catalog.feeders:
        print(f"  feeders: {len(catalog.feeders)} option(s)")
        feeder_preview = list(catalog.feeders[:20])
        print("    " + ", ".join(feeder_preview))
        if len(catalog.feeders) > 20:
            print("    ...")
    else:
        print("  feeders: <none>")


def prompt_select_from_values(label: str, values: Sequence[str], default: Optional[str] = None) -> str:
    if not values:
        raise ValueError(f"No values available to select for {label}.")

    print(f"Select {label}:")
    for index, value in enumerate(values, start=1):
        suffix = " (default)" if default is not None and value == default else ""
        print(f"  {index}. {value}{suffix}")

    default_index = None
    if default is not None:
        try:
            default_index = list(values).index(default) + 1
        except ValueError:
            default_index = None

    while True:
        prompt = f"Enter {label} number"
        if default_index is not None:
            prompt += f" [{default_index}]"
        prompt += ": "
        entered = input(prompt).strip()
        if not entered and default_index is not None:
            return values[default_index - 1]
        if entered.isdigit():
            selected = int(entered)
            if 1 <= selected <= len(values):
                return values[selected - 1]
        print(f"Invalid selection for {label}.")

def resolve_scenario(
    engine: Engine,
    work_package_id: str,
    year: int,
    feeders: Sequence[str],
    scenario: Optional[str],
) -> str:
    if scenario:
        return scenario

    year_start, year_end = _year_bounds(year)
    clause, has_feeders = _feeder_clause(feeders)
    stmt = text(
        f"""
        SELECT MIN(scenario) AS scenario
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
          AND "timestamp" >= :year_start
          AND "timestamp" < :year_end
          {clause}
        """
    )
    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))

    params = {
        "work_package_id": work_package_id,
        "year_start": year_start,
        "year_end": year_end,
    }
    if has_feeders:
        params["feeders"] = list(feeders)

    with engine.connect() as conn:
        value = conn.execute(stmt, params).scalar_one_or_none()

    if not value:
        raise ValueError("Could not resolve a scenario for the requested work package/year/scope.")

    return str(value)


def resolve_timestamp(
    engine: Engine,
    work_package_id: str,
    year: int,
    feeders: Sequence[str],
    scenario: str,
    timestamp: Optional[str],
) -> datetime:
    if timestamp:
        return _parse_timestamp(timestamp)

    year_start, year_end = _year_bounds(year)
    clause, has_feeders = _feeder_clause(feeders)
    stmt = text(
        f"""
        SELECT MIN("timestamp") AS ts
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
          AND scenario = :scenario
          AND "timestamp" >= :year_start
          AND "timestamp" < :year_end
          {clause}
        """
    )
    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))

    params = {
        "work_package_id": work_package_id,
        "scenario": scenario,
        "year_start": year_start,
        "year_end": year_end,
    }
    if has_feeders:
        params["feeders"] = list(feeders)

    with engine.connect() as conn:
        value = conn.execute(stmt, params).scalar_one_or_none()

    if value is None:
        raise ValueError("Could not resolve a timestamp for the requested work package/year/scenario/scope.")

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    return _parse_timestamp(str(value))


def resolve_slice_selection(
    engine: Engine,
    work_package_id: str,
    year: int,
    feeders: Sequence[str],
    scenario: Optional[str],
    timestamp: Optional[str],
    season: str,
    time_of_day: str,
) -> SliceSelection:
    resolved_scenario = resolve_scenario(
        engine=engine,
        work_package_id=work_package_id,
        year=year,
        feeders=feeders,
        scenario=scenario,
    )
    resolved_timestamp = resolve_timestamp(
        engine=engine,
        work_package_id=work_package_id,
        year=year,
        feeders=feeders,
        scenario=resolved_scenario,
        timestamp=timestamp,
    )

    return SliceSelection(
        work_package_id=work_package_id,
        year=year,
        scenario=resolved_scenario,
        timestamp=resolved_timestamp,
        season=season,
        time_of_day=time_of_day,
        feeders=tuple(sorted(set(feeders))),
    )


def _as_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_metric_rows(engine: Engine, selection: SliceSelection) -> List[DbMetricRow]:
    clause, has_feeders = _feeder_clause(selection.feeders)

    stmt = text(
        f"""
        SELECT
            work_package_id::text AS work_package_id,
            scenario,
            "timestamp",
            feeder::text AS feeder,
            measurement_zone_name,
            mz_type,
            conducting_equipment_mrid::text AS conducting_equipment_mrid,
            season,
            time_of_day,
            v_base,
            peak_import,
            peak_export,
            load_exceeding_normal_thermal_voltage_kwh,
            gen_exceeding_normal_thermal_voltage_kwh,
            v1_avg_section_voltage,
            v99_avg_section_voltage
        FROM public.network_performance_metrics_enhanced
        WHERE work_package_id = :work_package_id
          AND scenario = :scenario
          AND "timestamp" = :timestamp
          AND season = :season
          AND time_of_day = :time_of_day
          {clause}
        ORDER BY feeder::text, measurement_zone_name, conducting_equipment_mrid::text
        """
    )

    if has_feeders:
        stmt = stmt.bindparams(bindparam("feeders", expanding=True))

    params = {
        "work_package_id": selection.work_package_id,
        "scenario": selection.scenario,
        "timestamp": selection.timestamp,
        "season": selection.season,
        "time_of_day": selection.time_of_day,
    }
    if has_feeders:
        params["feeders"] = list(selection.feeders)

    with engine.connect() as conn:
        rows = conn.execute(stmt, params).mappings().all()

    parsed: List[DbMetricRow] = []
    for row in rows:
        ts = row["timestamp"]
        if isinstance(ts, datetime):
            ts_value = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        else:
            ts_value = _parse_timestamp(str(ts))

        parsed.append(
            DbMetricRow(
                work_package_id=str(row["work_package_id"]),
                scenario=str(row["scenario"]),
                timestamp=ts_value,
                feeder=str(row["feeder"]),
                measurement_zone_name=str(row["measurement_zone_name"]),
                mz_type=str(row["mz_type"]),
                conducting_equipment_mrid=str(row["conducting_equipment_mrid"]),
                season=str(row["season"]),
                time_of_day=str(row["time_of_day"]),
                v_base=_as_float(row["v_base"]),
                peak_import=_as_float(row["peak_import"]),
                peak_export=_as_float(row["peak_export"]),
                load_exceeding_normal_thermal_voltage_kwh=_as_float(row["load_exceeding_normal_thermal_voltage_kwh"]),
                gen_exceeding_normal_thermal_voltage_kwh=_as_float(row["gen_exceeding_normal_thermal_voltage_kwh"]),
                v1_avg_section_voltage=_as_float(row["v1_avg_section_voltage"]),
                v99_avg_section_voltage=_as_float(row["v99_avg_section_voltage"]),
            )
        )

    return parsed


async def build_feeder_trace_context(ewb: EwbSettings, feeder_mrid: str) -> FeederTraceContext:
    rpc_channel = _connect_rpc(ewb)
    client = NetworkConsumerClient(rpc_channel)
    result = None
    fetch_error = None
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        result = await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
        )
        if not result.was_failure:
            fetch_error = None
            break

        fetch_error = result.thrown
        if attempt < max_attempts and _is_deadline_exceeded_error(fetch_error):
            await asyncio.sleep(2 ** (attempt - 1))
            continue
        break

    if result is None or result.was_failure:
        raise RuntimeError(f"Failed to fetch feeder {feeder_mrid}: {fetch_error}")

    network = client.service
    await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)

    equipment_by_mrid: Dict[str, ConductingEquipment] = {}
    for equipment in network.objects(ConductingEquipment):
        equipment_by_mrid[str(equipment.mrid)] = equipment

    feeder_head: Optional[ConductingEquipment] = None
    for feeder in network.objects(Feeder):
        if str(feeder.mrid) != feeder_mrid:
            continue
        if feeder.normal_head_terminal and isinstance(feeder.normal_head_terminal.conducting_equipment, ConductingEquipment):
            feeder_head = feeder.normal_head_terminal.conducting_equipment
            break

    downstream_edges_by_from: DefaultDict[str, Set[str]] = defaultdict(set)
    traversed_lines_by_edge: DefaultDict[Tuple[str, str], Set[str]] = defaultdict(set)

    if feeder_head is not None:

        async def collect_edges(step: NetworkTraceStep, _):
            from_equipment = step.path.from_equipment
            to_equipment = step.path.to_equipment
            traversed = step.path.traversed_ac_line_segment
            if not isinstance(from_equipment, ConductingEquipment) or not isinstance(to_equipment, ConductingEquipment):
                return

            from_mrid = str(from_equipment.mrid)
            to_mrid = str(to_equipment.mrid)
            if from_mrid == to_mrid:
                return

            downstream_edges_by_from[from_mrid].add(to_mrid)
            if isinstance(traversed, AcLineSegment):
                traversed_lines_by_edge[(from_mrid, to_mrid)].add(str(traversed.mrid))

        await (
            Tracing.network_trace()
            .add_condition(downstream())
            .add_step_action(collect_edges)
        ).run(start=feeder_head, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return FeederTraceContext(
        feeder_mrid=feeder_mrid,
        equipment_by_mrid=equipment_by_mrid,
        downstream_edges_by_from={key: set(values) for key, values in downstream_edges_by_from.items()},
        traversed_lines_by_edge={key: set(values) for key, values in traversed_lines_by_edge.items()},
    )


def _trace_zone_segment_from_graph(
    start_mrid: str,
    downstream_edges_by_from: Mapping[str, Set[str]],
    traversed_lines_by_edge: Mapping[Tuple[str, str], Set[str]],
    boundary_head_ids: Set[str],
) -> Tuple[Set[str], Set[Tuple[str, str]]]:
    assets: Set[str] = {start_mrid}
    edges: Set[Tuple[str, str]] = set()
    visited: Set[str] = set()
    stack: List[str] = [start_mrid]

    while stack:
        current = stack.pop()
        if current in visited:
            continue
        visited.add(current)

        for downstream_mrid in downstream_edges_by_from.get(current, set()):
            edge = (current, downstream_mrid)
            edges.add(edge)

            for line_mrid in traversed_lines_by_edge.get(edge, set()):
                assets.add(line_mrid)

            if downstream_mrid in boundary_head_ids and downstream_mrid != start_mrid:
                continue

            assets.add(downstream_mrid)
            if downstream_mrid not in visited:
                stack.append(downstream_mrid)

    return assets, edges


async def map_zone_heads_to_assets(
    context: FeederTraceContext,
    head_ids: Set[str],
) -> Tuple[Dict[str, Set[str]], List[str]]:
    available_heads = {head_id for head_id in head_ids if head_id in context.equipment_by_mrid}
    missing_heads = sorted(head_ids.difference(available_heads))

    mapped: Dict[str, Set[str]] = {}
    for head_id in sorted(available_heads):
        boundaries = available_heads.difference({head_id})
        mapped[head_id], _ = _trace_zone_segment_from_graph(
            start_mrid=head_id,
            downstream_edges_by_from=context.downstream_edges_by_from,
            traversed_lines_by_edge=context.traversed_lines_by_edge,
            boundary_head_ids=boundaries,
        )

    return mapped, missing_heads


async def map_zone_heads_to_assets_and_edges(
    context: FeederTraceContext,
    head_ids: Set[str],
) -> Tuple[Dict[str, Set[str]], Dict[str, Set[Tuple[str, str]]], List[str]]:
    available_heads = {head_id for head_id in head_ids if head_id in context.equipment_by_mrid}
    missing_heads = sorted(head_ids.difference(available_heads))

    mapped_assets: Dict[str, Set[str]] = {}
    mapped_edges: Dict[str, Set[Tuple[str, str]]] = {}

    for head_id in sorted(available_heads):
        boundaries = available_heads.difference({head_id})
        assets, edges = _trace_zone_segment_from_graph(
            start_mrid=head_id,
            downstream_edges_by_from=context.downstream_edges_by_from,
            traversed_lines_by_edge=context.traversed_lines_by_edge,
            boundary_head_ids=boundaries,
        )
        mapped_assets[head_id] = assets
        mapped_edges[head_id] = edges

    return mapped_assets, mapped_edges, missing_heads


def to_equipment_geometry(equipment: ConductingEquipment):
    location = getattr(equipment, "location", None)
    points = list(location.points) if location is not None else []

    if len(points) > 1:
        coords = [(point.x_position, point.y_position) for point in points]
        return "line", LineString(coords)

    if len(points) == 1:
        point = points[0]
        return "point", Point((point.x_position, point.y_position))

    return None, None


def to_equipment_point(equipment: ConductingEquipment) -> Optional[Tuple[float, float]]:
    location = getattr(equipment, "location", None)
    points = list(location.points) if location is not None else []

    if not points:
        return None

    if len(points) == 1:
        p = points[0]
        return p.x_position, p.y_position

    mean_x = sum(p.x_position for p in points) / len(points)
    mean_y = sum(p.y_position for p in points) / len(points)
    return mean_x, mean_y


def to_edge_geometry(
    equipment_by_mrid: Dict[str, ConductingEquipment],
    from_mrid: str,
    to_mrid: str,
) -> Optional[LineString]:
    from_equipment = equipment_by_mrid.get(from_mrid)
    to_equipment = equipment_by_mrid.get(to_mrid)
    if from_equipment is None or to_equipment is None:
        return None

    from_point = to_equipment_point(from_equipment)
    to_point = to_equipment_point(to_equipment)
    if from_point is None or to_point is None:
        return None
    if from_point == to_point:
        return None

    return LineString([from_point, to_point])


def compute_percentiles(values: Sequence[float], probabilities: Sequence[float]) -> List[float]:
    if not values:
        return [0.0 for _ in probabilities]

    ordered = sorted(values)
    last_index = len(ordered) - 1
    output: List[float] = []

    for prob in probabilities:
        p = max(0.0, min(1.0, float(prob)))
        index = p * last_index
        low = int(math.floor(index))
        high = int(math.ceil(index))
        if low == high:
            output.append(ordered[low])
            continue
        fraction = index - low
        output.append(ordered[low] * (1.0 - fraction) + ordered[high] * fraction)

    return output


def bucket_from_thresholds(value: float, thresholds: Sequence[float]) -> int:
    if len(thresholds) != 4:
        raise ValueError("Expected exactly 4 thresholds.")

    if value <= thresholds[0]:
        return 0
    if value <= thresholds[1]:
        return 1
    if value <= thresholds[2]:
        return 2
    if value <= thresholds[3]:
        return 3
    return 4


def normalise(value: float, min_value: float, max_value: float, default: float = 0.5) -> float:
    spread = max_value - min_value
    if spread <= 1e-9:
        return default
    scaled = (value - min_value) / spread
    return max(0.0, min(1.0, scaled))
