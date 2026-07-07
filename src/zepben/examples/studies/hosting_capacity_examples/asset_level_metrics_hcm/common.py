#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from __future__ import annotations

import csv
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TypeVar


@dataclass(frozen=True)
class ThermalAssetSummary:
    work_package_id: str
    scenario: str
    year: int
    feeder: str
    conducting_equipment_mrid: str
    rating_unit: str
    normal_rating: Optional[float]
    emergency_rating: Optional[float]
    max_loading_pct: Optional[float]
    direction_at_max_loading: str
    max_current: Optional[float]
    max_kw: Optional[float]
    max_kvar: Optional[float]
    max_kva: Optional[float]
    hours_over_normal: Optional[float]
    hours_over_emergency: Optional[float]
    overload_kwh_import: Optional[float]
    overload_kwh_export: Optional[float]
    overload_pct_hours: Optional[float]
    avg_loading_pct_when_overloaded: Optional[float]
    worst_phase: str


@dataclass(frozen=True)
class VoltageAssetSummary:
    work_package_id: str
    scenario: str
    year: int
    feeder: str
    conducting_equipment_mrid: str
    phase: str
    v_base: Optional[float]
    min_upstream_voltage: Optional[float]
    max_upstream_voltage: Optional[float]
    avg_upstream_voltage: Optional[float]
    min_downstream_voltage: Optional[float]
    max_downstream_voltage: Optional[float]
    avg_downstream_voltage: Optional[float]
    min_delta: Optional[float]
    max_delta: Optional[float]
    hours_any_endpoint_below_limit: Optional[float]
    hours_any_endpoint_above_limit: Optional[float]
    abs_delta_hours: Optional[float]
    avg_abs_delta: Optional[float]


@dataclass(frozen=True)
class VoltageAssetWorstCase:
    row_count: int
    low_phase: str
    low_voltage_lg_v: float
    high_phase: str
    high_voltage_lg_v: float
    bandwidth_phase: str
    bandwidth_pct: float
    bandwidth_lg_v: float
    hours_below_limit: float
    hours_above_limit: float
    abs_delta_hours: float
    avg_abs_delta: Optional[float]
    v_base: Optional[float]


T = TypeVar("T")


def parse_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() == "NULL":
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def parse_int(value: object, default: int = 0) -> int:
    parsed = parse_float(value)
    if parsed is None:
        return default
    return int(parsed)


def max_non_null(values: Iterable[object]) -> Optional[float]:
    parsed = [value for value in (parse_float(value) for value in values) if value is not None]
    return max(parsed) if parsed else None


def min_non_null(values: Iterable[object]) -> Optional[float]:
    parsed = [value for value in (parse_float(value) for value in values) if value is not None]
    return min(parsed) if parsed else None


def _read_csv_rows(path: Path) -> List[Mapping[str, str]]:
    with open(path, "r", newline="") as file:
        return list(csv.DictReader(file))


def load_thermal_asset_summaries(path: str | Path) -> List[ThermalAssetSummary]:
    rows: List[ThermalAssetSummary] = []
    for row in _read_csv_rows(Path(path)):
        rows.append(
            ThermalAssetSummary(
                work_package_id=str(row["work_package_id"]),
                scenario=str(row["scenario"]),
                year=parse_int(row["year"]),
                feeder=str(row["feeder"]),
                conducting_equipment_mrid=str(row["conducting_equipment_mrid"]),
                rating_unit=str(row.get("rating_unit") or ""),
                normal_rating=parse_float(row.get("normal_rating")),
                emergency_rating=parse_float(row.get("emergency_rating")),
                max_loading_pct=parse_float(row.get("max_loading_pct")),
                direction_at_max_loading=str(row.get("direction_at_max_loading") or ""),
                max_current=parse_float(row.get("max_current")),
                max_kw=parse_float(row.get("max_kw")),
                max_kvar=parse_float(row.get("max_kvar")),
                max_kva=parse_float(row.get("max_kva")),
                hours_over_normal=parse_float(row.get("hours_over_normal")),
                hours_over_emergency=parse_float(row.get("hours_over_emergency")),
                overload_kwh_import=parse_float(row.get("overload_kwh_import")),
                overload_kwh_export=parse_float(row.get("overload_kwh_export")),
                overload_pct_hours=parse_float(row.get("overload_pct_hours")),
                avg_loading_pct_when_overloaded=parse_float(row.get("avg_loading_pct_when_overloaded")),
                worst_phase=str(row.get("worst_phase") or ""),
            )
        )
    return rows


def load_voltage_asset_summaries(path: str | Path) -> List[VoltageAssetSummary]:
    rows: List[VoltageAssetSummary] = []
    for row in _read_csv_rows(Path(path)):
        rows.append(
            VoltageAssetSummary(
                work_package_id=str(row["work_package_id"]),
                scenario=str(row["scenario"]),
                year=parse_int(row["year"]),
                feeder=str(row["feeder"]),
                conducting_equipment_mrid=str(row["conducting_equipment_mrid"]),
                phase=str(row.get("phase") or ""),
                v_base=parse_float(row.get("v_base")),
                min_upstream_voltage=parse_float(row.get("min_upstream_voltage")),
                max_upstream_voltage=parse_float(row.get("max_upstream_voltage")),
                avg_upstream_voltage=parse_float(row.get("avg_upstream_voltage")),
                min_downstream_voltage=parse_float(row.get("min_downstream_voltage")),
                max_downstream_voltage=parse_float(row.get("max_downstream_voltage")),
                avg_downstream_voltage=parse_float(row.get("avg_downstream_voltage")),
                min_delta=parse_float(row.get("min_delta")),
                max_delta=parse_float(row.get("max_delta")),
                hours_any_endpoint_below_limit=parse_float(row.get("hours_any_endpoint_below_limit")),
                hours_any_endpoint_above_limit=parse_float(row.get("hours_any_endpoint_above_limit")),
                abs_delta_hours=parse_float(row.get("abs_delta_hours")),
                avg_abs_delta=parse_float(row.get("avg_abs_delta")),
            )
        )
    return rows


def load_cim_to_opendss_mapping(path: str | Path) -> Dict[str, Tuple[str, ...]]:
    by_simplified: Dict[str, set[str]] = defaultdict(set)
    for row in _read_csv_rows(Path(path)):
        simplified = str(row.get("simplified_mrid") or "").strip()
        original = str(row.get("original_mrid") or "").strip()
        original_type = str(row.get("original_type") or "").strip()
        if not simplified or not original:
            continue
        if original_type not in {"AcLineSegment", "PowerTransformer"}:
            continue
        by_simplified[simplified].add(original)

    return {key: tuple(sorted(values)) for key, values in by_simplified.items()}


def expand_simplified_asset_mrids(
    result_mrid: str,
    simplified_to_original: Mapping[str, Sequence[str]],
) -> Tuple[str, ...]:
    mapped = tuple(sorted(set(simplified_to_original.get(result_mrid, ()))))
    return mapped if mapped else (result_mrid,)


def filter_records(
    rows: Sequence[T],
    work_package_id: Optional[str],
    scenario: Optional[str],
    year: Optional[int],
    feeders: Sequence[str],
) -> List[T]:
    feeder_filter = set(feeders)
    selected: List[T] = []
    for row in rows:
        if work_package_id and getattr(row, "work_package_id") != work_package_id:
            continue
        if scenario and getattr(row, "scenario") != scenario:
            continue
        if year is not None and getattr(row, "year") != year:
            continue
        if feeder_filter and getattr(row, "feeder") not in feeder_filter:
            continue
        selected.append(row)
    return selected


def thermal_loading_bucket(max_loading_pct: Optional[float]) -> int:
    if max_loading_pct is None:
        return 0
    if max_loading_pct < 90.0:
        return 0
    if max_loading_pct < 100.0:
        return 1
    if max_loading_pct < 130.0:
        return 2
    return 3


def thermal_loading_label(bucket: int) -> str:
    return {
        0: "Below 90%",
        1: "90-100%",
        2: "100-130%",
        3: "Above 130%",
    }.get(bucket, "Unknown")


def voltage_line_ground(pu_value: Optional[float], v_base: Optional[float]) -> Optional[float]:
    if pu_value is None or v_base is None or v_base <= 0:
        return None
    return float(pu_value) * (float(v_base) / math.sqrt(3.0))


def voltage_low_bucket(voltage_lg_v: Optional[float]) -> int:
    if voltage_lg_v is None:
        return 0
    if voltage_lg_v < 216.0:
        return 3
    if voltage_lg_v < 225.0:
        return 2
    if voltage_lg_v < 230.0:
        return 1
    return 0


def voltage_high_bucket(voltage_lg_v: Optional[float]) -> int:
    if voltage_lg_v is None:
        return 0
    if voltage_lg_v > 253.0:
        return 3
    if voltage_lg_v > 245.0:
        return 2
    if voltage_lg_v > 240.0:
        return 1
    return 0


def voltage_bandwidth_bucket(bandwidth_pct: Optional[float]) -> int:
    if bandwidth_pct is None:
        return 0
    if bandwidth_pct < 3.0:
        return 0
    if bandwidth_pct < 6.0:
        return 1
    if bandwidth_pct < 10.0:
        return 2
    return 3


def _minimum_endpoint_voltage(row: VoltageAssetSummary) -> Optional[float]:
    return min_non_null([row.min_upstream_voltage, row.min_downstream_voltage])


def _maximum_endpoint_voltage(row: VoltageAssetSummary) -> Optional[float]:
    return max_non_null([row.max_upstream_voltage, row.max_downstream_voltage])


def _bandwidth_pct(row: VoltageAssetSummary) -> Optional[float]:
    low = _minimum_endpoint_voltage(row)
    high = _maximum_endpoint_voltage(row)
    if low is None or high is None:
        return None
    return max(0.0, (high - low) * 100.0)


def worst_voltage_by_asset(rows: Sequence[VoltageAssetSummary]) -> Dict[str, VoltageAssetWorstCase]:
    grouped: Dict[str, List[VoltageAssetSummary]] = defaultdict(list)
    for row in rows:
        grouped[row.conducting_equipment_mrid].append(row)

    summaries: Dict[str, VoltageAssetWorstCase] = {}
    for asset_mrid, asset_rows in grouped.items():
        lows = [
            (voltage_line_ground(_minimum_endpoint_voltage(row), row.v_base), row)
            for row in asset_rows
        ]
        highs = [
            (voltage_line_ground(_maximum_endpoint_voltage(row), row.v_base), row)
            for row in asset_rows
        ]
        bandwidths = [
            (_bandwidth_pct(row), row)
            for row in asset_rows
        ]

        valid_lows = [(value, row) for value, row in lows if value is not None]
        valid_highs = [(value, row) for value, row in highs if value is not None]
        valid_bandwidths = [(value, row) for value, row in bandwidths if value is not None]
        if not valid_lows or not valid_highs or not valid_bandwidths:
            continue

        low_value, low_row = min(valid_lows, key=lambda item: item[0])
        high_value, high_row = max(valid_highs, key=lambda item: item[0])
        bandwidth_pct, bandwidth_row = max(valid_bandwidths, key=lambda item: item[0])

        bandwidth_lg = 0.0
        if bandwidth_row.v_base is not None:
            bandwidth_lg = float(bandwidth_pct) * (float(bandwidth_row.v_base) / math.sqrt(3.0)) / 100.0

        below_hours = sum(row.hours_any_endpoint_below_limit or 0.0 for row in asset_rows)
        above_hours = sum(row.hours_any_endpoint_above_limit or 0.0 for row in asset_rows)
        abs_delta_hours = sum(row.abs_delta_hours or 0.0 for row in asset_rows)
        avg_abs_delta = max_non_null(row.avg_abs_delta for row in asset_rows)
        v_base = max_non_null(row.v_base for row in asset_rows)

        summaries[asset_mrid] = VoltageAssetWorstCase(
            row_count=len(asset_rows),
            low_phase=low_row.phase,
            low_voltage_lg_v=float(low_value),
            high_phase=high_row.phase,
            high_voltage_lg_v=float(high_value),
            bandwidth_phase=bandwidth_row.phase,
            bandwidth_pct=float(bandwidth_pct),
            bandwidth_lg_v=float(bandwidth_lg),
            hours_below_limit=below_hours,
            hours_above_limit=above_hours,
            abs_delta_hours=abs_delta_hours,
            avg_abs_delta=avg_abs_delta,
            v_base=v_base,
        )

    return summaries
