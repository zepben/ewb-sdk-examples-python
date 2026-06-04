#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import argparse
import asyncio
import json
import math
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from geojson import Feature, FeatureCollection
from geojson.geometry import LineString, Point
from zepben.eas import Mutation, GeoJsonOverlayInput, StudyResultInput, StudyInput
from zepben.examples.studies.study_utils import create_eas_client_from_config, connect_rpc_from_config
from zepben.ewb import (
    AcLineSegment,
    ConductingEquipment,
    EnergyConsumer,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    NetworkTraceStep,
    PhaseCode,
    PhotoVoltaicUnit,
    PowerElectronicsConnection,
    PowerTransformer,
    Tracing,
    connect_with_token,
    downstream,
    stop_at_open,
    upstream,
)
from zepben.ewb.services.network.tracing.networktrace.operators.network_state_operators import NetworkStateOperators


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config.json"
STYLE_PATH = Path(__file__).resolve().parent / "style_mock_zone_analytics.json"

# ADMD / ADMG assumptions requested by user.
ADMD_KW_PER_CUSTOMER = 4.0
ADMG_KW_PER_CUSTOMER = 5.0


USE_CASE_NAMES: Dict[int, str] = {
    6: "Neutral Integrity Fault Detection",
    7: "Voltage Monitoring and Reporting",
    8: "Dynamic Voltage Control",
    9: "Phase Identification",
    13: "CER Compliance",
    14: "CER Performance",
    15: "EV Charger Detection",
}


@dataclass(frozen=True)
class FeederRef:
    zone_code: str
    feeder_mrid: str
    feeder_name: str


@dataclass
class CustomerAnalytics:
    mrid: str
    feeder_mrid: str
    feeder_name: str
    zone_code: str
    lon: float
    lat: float
    phase_class: str
    has_pv: bool
    transformer_mrid: str
    tx_impedance_proxy: float
    tx_load_util_pct: float
    tx_generation_util_pct: float
    tx_est_voltage_pct: float
    tx_pv_penetration_pct: float
    tx_pec_penetration_pct: float


@dataclass
class TransformerAnalytics:
    mrid: str
    name: str
    feeder_mrid: str
    feeder_name: str
    zone_code: str
    lon: Optional[float]
    lat: Optional[float]
    ec_count: int
    pv_ec_count: int
    pv_capacity_kva: float
    pec_count: int
    pec_capacity_kva: float
    rating_kva: float
    line_km: float
    mean_z_ohm_per_km: float
    impedance_proxy: float
    load_util_pct: float
    generation_util_pct: float
    headroom_kw: float
    upstream_mv_line_ids: Set[str] = field(default_factory=set)
    path_mv_impedance_ohm: float = 0.0
    estimated_mv_voltage_pct: float = 100.0
    tap_from: int = 0
    tap_to: int = 0
    tap_action: str = "Hold"
    tap_change_required: bool = False
    curtailment_kwh: float = 0.0

    @property
    def pv_ratio(self) -> float:
        if self.ec_count <= 0:
            return 0.0
        return self.pv_ec_count / self.ec_count

    @property
    def pv_penetration_pct_of_rating(self) -> float:
        return (self.pv_capacity_kva / max(self.rating_kva, 1.0)) * 100.0

    @property
    def pec_penetration_pct_of_rating(self) -> float:
        return (self.pec_capacity_kva / max(self.rating_kva, 1.0)) * 100.0


@dataclass
class MvLineAnalytics:
    mrid: str
    name: str
    feeder_mrid: str
    feeder_name: str
    zone_code: str
    coordinates: List[Tuple[float, float]]
    length_km: float
    z_ohm_per_km: float
    base_kv: float
    downstream_ec_count: int
    downstream_pv_count: int
    net_kw: float
    signed_voltage_change_pct: float
    estimated_end_voltage_pct: float
    tap_zone: str


@dataclass
class FeederAnalytics:
    ref: FeederRef
    customers: List[CustomerAnalytics]
    transformers: List[TransformerAnalytics]
    mv_lines: List[MvLineAnalytics]

    @property
    def total_ec(self) -> int:
        return len(self.customers)

    @property
    def total_pv_ec(self) -> int:
        return sum(1 for c in self.customers if c.has_pv)

    @property
    def pv_ratio(self) -> float:
        if self.total_ec <= 0:
            return 0.0
        return self.total_pv_ec / self.total_ec


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a correlated mock analytics study using live EWB network data. "
            "Each use case is published as its own result layer."
        )
    )
    parser.add_argument("--zones", default="CPM", help="Comma-separated zone MRIDs to analyse.")
    parser.add_argument("--config", default=str(CONFIG_PATH), help="Path to config.json.")
    parser.add_argument("--seed", type=int, default=20260219, help="Deterministic seed for mock generation.")
    parser.add_argument("--batch-size", type=int, default=2, help="Feeders analysed in parallel.")
    parser.add_argument("--period-start", default="2025-01-01", help="Historic start date.")
    parser.add_argument("--period-end", default="2025-12-31", help="Historic end date.")
    parser.add_argument("--name", default="Zone Substation Analytics Demo", help="Study name.")
    parser.add_argument("--dry-run", action="store_true", help="Generate outputs but do not upload.")
    return parser.parse_args(argv)


def _load_config(path: str) -> Dict:
    with open(path, "r") as f:
        return json.loads(f.read())


def _load_styles() -> List[Dict]:
    with open(STYLE_PATH, "r") as f:
        return json.loads(f.read())


def _chunk(items: Sequence, size: int) -> Iterable[Sequence]:
    safe = max(size, 1)
    for i in range(0, len(items), safe):
        yield items[i : i + safe]


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(value, high))


def _rng(seed: int, *parts: str) -> random.Random:
    return random.Random(":".join([str(seed), *parts]))


@dataclass(frozen=True)
class Normaliser:
    min_value: float
    max_value: float

    def scale(self, value: float, default: float = 0.5) -> float:
        spread = self.max_value - self.min_value
        if spread <= 1e-9:
            return default
        return _clamp((value - self.min_value) / spread, 0.0, 1.0)


def _normaliser(values: Iterable[float]) -> Normaliser:
    vals = list(values)
    if not vals:
        return Normaliser(0.0, 1.0)
    return Normaliser(min(vals), max(vals))


async def _collect_feeders(config: Dict, zone_codes: Sequence[str]) -> List[FeederRef]:
    rpc_channel = connect_rpc_from_config(config)
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()
    substations = hierarchy.value.substations

    feeder_refs: List[FeederRef] = []
    missing: List[str] = []
    for zone in zone_codes:
        zone_data = substations.get(zone)
        if zone_data is None:
            missing.append(zone)
            continue
        for feeder in zone_data.feeders:
            feeder_refs.append(
                FeederRef(
                    zone_code=zone,
                    feeder_mrid=feeder.mrid,
                    feeder_name=(getattr(feeder, "name", None) or feeder.mrid),
                )
            )

    if missing:
        available = ", ".join(sorted(substations.keys()))
        raise ValueError(f"Unknown zone code(s): {', '.join(missing)}. Available zones include: {available}")

    return feeder_refs


def _point_from_location(location) -> Optional[Tuple[float, float]]:
    points = list(location.points) if location is not None else []
    if not points:
        return None
    if len(points) == 1:
        return points[0].x_position, points[0].y_position
    x = sum(p.x_position for p in points) / len(points)
    y = sum(p.y_position for p in points) / len(points)
    return x, y


def _line_coords(location) -> List[Tuple[float, float]]:
    points = list(location.points) if location is not None else []
    if len(points) < 2:
        return []
    return [(p.x_position, p.y_position) for p in points]


def _distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(d_lon / 2) ** 2
    )
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def _location_length_km(location) -> float:
    points = list(location.points) if location is not None else []
    if len(points) < 2:
        return 0.0
    length = 0.0
    for i in range(1, len(points)):
        p1 = points[i - 1]
        p2 = points[i]
        length += _distance_km(p1.x_position, p1.y_position, p2.x_position, p2.y_position)
    return length


def _line_length_km(line: AcLineSegment) -> float:
    if line.length is not None and line.length > 0:
        return line.length / 1000.0
    return _location_length_km(getattr(line, "location", None))


def _line_impedance_ohm_per_km(line: AcLineSegment) -> float:
    for attr in ("per_length_sequence_impedance", "per_length_phase_impedance", "per_length_impedance"):
        imp = getattr(line, attr, None)
        if imp is None:
            continue
        r = getattr(imp, "r", None)
        x = getattr(imp, "x", None)
        if r is None and x is None:
            continue
        return math.hypot(float(r or 0.0), float(x or 0.0)) * 1000.0
    return 0.0


def _line_base_kv(line: AcLineSegment) -> float:
    base_value = getattr(line, "base_voltage_value", None)
    if base_value is not None and base_value > 0:
        return float(base_value) / 1000.0
    base = getattr(line, "base_voltage", None)
    nominal = getattr(base, "nominal_voltage", None) if base is not None else None
    if nominal is not None and nominal > 0:
        return float(nominal) / 1000.0
    return 11.0


def _is_mv_line(line: AcLineSegment) -> bool:
    return _line_base_kv(line) > 1.0


def _transformer_rating_kva(pt: PowerTransformer) -> float:
    ratings: List[float] = []
    for end in pt.ends:
        if end.rated_s is not None:
            ratings.append(float(end.rated_s) / 1000.0)
        else:
            for rating in end.s_ratings:
                if rating and rating.rated_s is not None:
                    ratings.append(float(rating.rated_s) / 1000.0)
    return max(ratings) if ratings else 315.0


def _pec_has_pv_unit(pec: PowerElectronicsConnection) -> bool:
    return any(isinstance(unit, PhotoVoltaicUnit) for unit in pec.units)


def _pec_capacity_kva(pec: PowerElectronicsConnection) -> float:
    if pec.rated_s is not None and pec.rated_s > 0:
        return float(pec.rated_s) / 1000.0
    unit_capacity = sum(float(unit.max_p or 0.0) for unit in pec.units)
    return (unit_capacity / 1000.0) if unit_capacity > 0 else 0.0


def _phase_class(ec: EnergyConsumer) -> str:
    def classify(phase_obj) -> Optional[str]:
        if phase_obj is None:
            return None

        candidates: List[str] = [str(phase_obj).upper()]
        try:
            as_code = phase_obj.as_phase_code() if hasattr(phase_obj, "as_phase_code") else None
            if as_code is not None:
                candidates.append(str(as_code).upper())
        except Exception:
            pass

        for text in candidates:
            token = text.split(".")[-1]
            phase_token = "".join(ch for ch in token if ch in {"A", "B", "C", "N"})
            letters = {ch for ch in phase_token if ch in {"A", "B", "C"}}
            has_n = "N" in phase_token
            if letters == {"A"} and has_n:
                return "A"
            if letters == {"B"} and has_n:
                return "B"
            if letters == {"C"} and has_n:
                return "C"
            if letters == {"A"}:
                return "A"
            if letters == {"B"}:
                return "B"
            if letters == {"C"}:
                return "C"
            if len(letters) >= 2:
                return "OTHER"
        return None

    for terminal in ec.terminals:
        # Prefer static terminal phase code over dynamic NormalPhases state.
        phase = classify(getattr(terminal, "phases", None))
        if phase is not None:
            return phase
        phase = classify(getattr(terminal, "normal_phases", None))
        if phase is not None:
            return phase
    return "OTHER"


def _sample_nominal_tap(rng: random.Random) -> int:
    # Keep most transformers near nominal tap 4 with 90% between 3 and 7.
    r = rng.random()
    if r < 0.66:
        return 4
    if r < 0.80:
        return 3
    if r < 0.92:
        return 5
    if r < 0.96:
        return 6
    if r < 0.985:
        return 7
    if r < 0.995:
        return 2
    return 1


def _collect_eq_provider(collection: Set[ConductingEquipment]):
    async def collect(ps: NetworkTraceStep, _):
        to_eq = ps.path.to_equipment
        if to_eq is not None:
            collection.add(to_eq)

    return collect


async def _get_downstream_equipment(start: ConductingEquipment) -> Set[ConductingEquipment]:
    equipment_set: Set[ConductingEquipment] = set()
    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(_collect_eq_provider(equipment_set))
    ).run(start=start, phases=PhaseCode.ABCN, can_stop_on_start_item=False)
    return equipment_set


def _collect_line_provider(lines: Set[AcLineSegment]):
    async def collect(ps: NetworkTraceStep, _):
        traversed = ps.path.traversed_ac_line_segment
        if isinstance(traversed, AcLineSegment):
            lines.add(traversed)
        if isinstance(ps.path.to_equipment, AcLineSegment):
            lines.add(ps.path.to_equipment)
        if isinstance(ps.path.from_equipment, AcLineSegment):
            lines.add(ps.path.from_equipment)

    return collect


async def _get_upstream_lines(start: ConductingEquipment) -> Set[AcLineSegment]:
    lines: Set[AcLineSegment] = set()
    await (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_condition(stop_at_open())
        .add_step_action(_collect_line_provider(lines))
    ).run(start=start, phases=PhaseCode.ABCN, can_stop_on_start_item=False)
    return lines


def _find_pv_energy_consumers(network) -> Set[str]:
    pv_usage_points: Set[str] = set()
    pv_nodes: Set[str] = set()

    for pv in network.objects(PhotoVoltaicUnit):
        for up in pv.usage_points:
            pv_usage_points.add(up.mrid)
        if pv.power_electronics_connection:
            for up in pv.power_electronics_connection.usage_points:
                pv_usage_points.add(up.mrid)

    for pec in network.objects(PowerElectronicsConnection):
        if not _pec_has_pv_unit(pec):
            continue
        for up in pec.usage_points:
            pv_usage_points.add(up.mrid)
        for terminal in pec.terminals:
            if terminal.connectivity_node:
                pv_nodes.add(terminal.connectivity_node.mrid)

    pv_consumers: Set[str] = set()
    for ec in network.objects(EnergyConsumer):
        if any(up.mrid in pv_usage_points for up in ec.usage_points):
            pv_consumers.add(ec.mrid)
            continue
        if pv_nodes and any(t.connectivity_node and t.connectivity_node.mrid in pv_nodes for t in ec.terminals):
            pv_consumers.add(ec.mrid)

    return pv_consumers


async def _analyse_feeder(ref: FeederRef, config: Dict, seed: int) -> Optional[FeederAnalytics]:
    print(f"Fetching feeder {ref.feeder_name} ({ref.feeder_mrid})")
    rpc_channel = connect_rpc_from_config(config)
    client = NetworkConsumerClient(rpc_channel)
    result = await client.get_equipment_container(
        mrid=ref.feeder_mrid,
        expected_class=Feeder,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
    )
    if result.was_failure:
        print(f"Failed feeder {ref.feeder_mrid}: {result.thrown}")
        return None

    network = client.service
    await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)

    pv_consumers = _find_pv_energy_consumers(network)

    ec_objects = list(network.objects(EnergyConsumer))
    ec_locations: Dict[str, Tuple[EnergyConsumer, Tuple[float, float]]] = {}
    for ec in ec_objects:
        point = _point_from_location(ec.location)
        if point is not None:
            ec_locations[ec.mrid] = (ec, point)

    transformers: List[TransformerAnalytics] = []
    tx_by_id: Dict[str, TransformerAnalytics] = {}
    ec_to_tx: Dict[str, str] = {}

    line_down_ec: Dict[str, int] = {}
    line_down_pv: Dict[str, int] = {}

    tx_objects = list(network.objects(PowerTransformer))
    print(f"Tracing {len(tx_objects)} transformers for feeder {ref.feeder_name}")

    for pt in tx_objects:
        downstream_eq = await _get_downstream_equipment(pt)
        downstream_ecs = [eq for eq in downstream_eq if isinstance(eq, EnergyConsumer)]
        downstream_lines = [eq for eq in downstream_eq if isinstance(eq, AcLineSegment)]
        downstream_pecs = [eq for eq in downstream_eq if isinstance(eq, PowerElectronicsConnection)]

        ec_count = len(downstream_ecs)
        pv_ec_count = sum(1 for ec in downstream_ecs if ec.mrid in pv_consumers)

        pv_pecs = [pec for pec in downstream_pecs if _pec_has_pv_unit(pec)]
        pv_capacity_kva = sum(_pec_capacity_kva(pec) for pec in pv_pecs)
        pec_capacity_kva = sum(_pec_capacity_kva(pec) for pec in downstream_pecs)

        line_km = 0.0
        weighted_impedance = 0.0
        for line in downstream_lines:
            length_km = _line_length_km(line)
            z = _line_impedance_ohm_per_km(line)
            line_km += length_km
            if z > 0 and length_km > 0:
                weighted_impedance += z * length_km

        mean_z = weighted_impedance / max(line_km, 0.001) if weighted_impedance > 0 else 0.35
        impedance_proxy = mean_z * (line_km / max(ec_count, 1))

        rating_kva = _transformer_rating_kva(pt)
        est_peak_kw = ec_count * ADMD_KW_PER_CUSTOMER
        est_reverse_kw = pv_ec_count * ADMG_KW_PER_CUSTOMER
        load_util_pct = _clamp((est_peak_kw / max(rating_kva, 1.0)) * 100.0, 0.0, 240.0)
        generation_util_pct = _clamp((est_reverse_kw / max(rating_kva, 1.0)) * 100.0, 0.0, 240.0)

        point = _point_from_location(pt.location)
        tx = TransformerAnalytics(
            mrid=pt.mrid,
            name=pt.name or pt.mrid,
            feeder_mrid=ref.feeder_mrid,
            feeder_name=ref.feeder_name,
            zone_code=ref.zone_code,
            lon=(point[0] if point is not None else None),
            lat=(point[1] if point is not None else None),
            ec_count=ec_count,
            pv_ec_count=pv_ec_count,
            pv_capacity_kva=pv_capacity_kva,
            pec_count=len(downstream_pecs),
            pec_capacity_kva=pec_capacity_kva,
            rating_kva=rating_kva,
            line_km=line_km,
            mean_z_ohm_per_km=mean_z,
            impedance_proxy=impedance_proxy,
            load_util_pct=load_util_pct,
            generation_util_pct=generation_util_pct,
            headroom_kw=rating_kva - est_peak_kw,
        )

        upstream_lines = await _get_upstream_lines(pt)
        mv_upstream_lines = [line for line in upstream_lines if _is_mv_line(line)]
        mv_line_ids = {line.mrid for line in mv_upstream_lines}
        tx.upstream_mv_line_ids = mv_line_ids
        tx.path_mv_impedance_ohm = sum(
            max(_line_impedance_ohm_per_km(line), 0.35) * max(_line_length_km(line), 0.01)
            for line in mv_upstream_lines
        )

        for line_id in mv_line_ids:
            line_down_ec[line_id] = line_down_ec.get(line_id, 0) + ec_count
            line_down_pv[line_id] = line_down_pv.get(line_id, 0) + pv_ec_count

        for ec in downstream_ecs:
            if ec.mrid in ec_locations and ec.mrid not in ec_to_tx:
                ec_to_tx[ec.mrid] = pt.mrid

        transformers.append(tx)
        tx_by_id[tx.mrid] = tx

    mv_lines: List[MvLineAnalytics] = []
    line_drop_map: Dict[str, float] = {}
    for line in network.objects(AcLineSegment):
        if not _is_mv_line(line):
            continue
        coords = _line_coords(line.location)
        if len(coords) < 2:
            continue

        length_km = max(_line_length_km(line), 0.01)
        z_ohm_per_km = _line_impedance_ohm_per_km(line)
        if z_ohm_per_km <= 0:
            z_ohm_per_km = 0.35

        down_ec = line_down_ec.get(line.mrid, 0)
        down_pv = line_down_pv.get(line.mrid, 0)

        net_kw = down_ec * ADMD_KW_PER_CUSTOMER - down_pv * ADMG_KW_PER_CUSTOMER
        base_kv = max(_line_base_kv(line), 1.0)
        current_a = abs(net_kw) / (math.sqrt(3.0) * base_kv)
        total_line_ohm = z_ohm_per_km * length_km
        voltage_change_v = current_a * total_line_ohm
        voltage_change_pct = (voltage_change_v / (base_kv * 1000.0)) * 100.0
        signed_change_pct = voltage_change_pct if net_kw >= 0 else -voltage_change_pct
        est_end_v_pct = 100.0 - signed_change_pct

        if est_end_v_pct < 99.0:
            tap_zone = "Raise"
        elif est_end_v_pct > 101.0:
            tap_zone = "Lower"
        else:
            tap_zone = "Hold"

        line_drop_map[line.mrid] = signed_change_pct
        mv_lines.append(
            MvLineAnalytics(
                mrid=line.mrid,
                name=line.name or line.mrid,
                feeder_mrid=ref.feeder_mrid,
                feeder_name=ref.feeder_name,
                zone_code=ref.zone_code,
                coordinates=coords,
                length_km=length_km,
                z_ohm_per_km=z_ohm_per_km,
                base_kv=base_kv,
                downstream_ec_count=down_ec,
                downstream_pv_count=down_pv,
                net_kw=net_kw,
                signed_voltage_change_pct=signed_change_pct,
                estimated_end_voltage_pct=est_end_v_pct,
                tap_zone=tap_zone,
            )
        )

    for tx in transformers:
        path_change = sum(line_drop_map.get(line_id, 0.0) for line_id in tx.upstream_mv_line_ids)
        tx.estimated_mv_voltage_pct = 100.0 - path_change

    path_norm = _normaliser(tx.path_mv_impedance_ohm for tx in transformers)
    dev_norm = _normaliser(abs(tx.estimated_mv_voltage_pct - 100.0) for tx in transformers)
    load_norm = _normaliser(tx.load_util_pct for tx in transformers)

    for tx in transformers:
        tap_rng = _rng(seed, "tap", tx.mrid)
        tx.tap_from = _sample_nominal_tap(tap_rng)

        change_score = (
            0.62 * path_norm.scale(tx.path_mv_impedance_ohm)
            + 0.23 * dev_norm.scale(abs(tx.estimated_mv_voltage_pct - 100.0))
            + 0.15 * load_norm.scale(tx.load_util_pct)
            + tap_rng.uniform(-0.08, 0.08)
        )

        should_consider_change = (
            change_score > 0.72
            and (
                abs(tx.estimated_mv_voltage_pct - 100.0) > 0.22
                or path_norm.scale(tx.path_mv_impedance_ohm) > 0.82
            )
        )

        if should_consider_change:
            if tx.estimated_mv_voltage_pct < 99.2:
                tx.tap_to = int(_clamp(tx.tap_from + 1, 1, 7))
                tx.tap_action = "Raise"
            elif tx.estimated_mv_voltage_pct > 100.9:
                tx.tap_to = int(_clamp(tx.tap_from - 1, 1, 7))
                tx.tap_action = "Lower"
            elif tx.generation_util_pct > tx.load_util_pct:
                tx.tap_to = int(_clamp(tx.tap_from - 1, 1, 7))
                tx.tap_action = "Lower"
            else:
                tx.tap_to = int(_clamp(tx.tap_from + 1, 1, 7))
                tx.tap_action = "Raise"
        else:
            tx.tap_to = tx.tap_from
            tx.tap_action = "Hold"

        tx.tap_change_required = tx.tap_to != tx.tap_from

        pec_pct = tx.pec_penetration_pct_of_rating
        tx.curtailment_kwh = max(0.0, (pec_pct - 28.0)) * (16.0 + tx.pv_ec_count * 1.4)

    if not transformers:
        print(f"Skipping feeder {ref.feeder_mrid}: no transformers found")
        return None

    mean_tx_impedance = sum(t.impedance_proxy for t in transformers) / len(transformers)
    mean_load_util = sum(t.load_util_pct for t in transformers) / len(transformers)
    mean_gen_util = sum(t.generation_util_pct for t in transformers) / len(transformers)
    mean_tx_voltage = sum(t.estimated_mv_voltage_pct for t in transformers) / len(transformers)

    customers: List[CustomerAnalytics] = []
    for ec_mrid, (ec, point) in ec_locations.items():
        tx_id = ec_to_tx.get(ec_mrid)
        tx = tx_by_id.get(tx_id) if tx_id else None
        customers.append(
            CustomerAnalytics(
                mrid=ec_mrid,
                feeder_mrid=ref.feeder_mrid,
                feeder_name=ref.feeder_name,
                zone_code=ref.zone_code,
                lon=point[0],
                lat=point[1],
                phase_class=_phase_class(ec),
                has_pv=(ec_mrid in pv_consumers),
                transformer_mrid=(tx.mrid if tx is not None else "unknown"),
                tx_impedance_proxy=(tx.impedance_proxy if tx is not None else mean_tx_impedance),
                tx_load_util_pct=(tx.load_util_pct if tx is not None else mean_load_util),
                tx_generation_util_pct=(tx.generation_util_pct if tx is not None else mean_gen_util),
                tx_est_voltage_pct=(tx.estimated_mv_voltage_pct if tx is not None else mean_tx_voltage),
                tx_pv_penetration_pct=(tx.pv_penetration_pct_of_rating if tx is not None else 0.0),
                tx_pec_penetration_pct=(tx.pec_penetration_pct_of_rating if tx is not None else 0.0),
            )
        )

    if not customers:
        print(f"Skipping feeder {ref.feeder_mrid}: no mappable customers")
        return None

    return FeederAnalytics(
        ref=ref,
        customers=customers,
        transformers=transformers,
        mv_lines=mv_lines,
    )


def _build_uc6_neutral_faults(feeders: Sequence[FeederAnalytics], seed: int, period_start: str, period_end: str) -> FeatureCollection:
    features: List[Feature] = []
    for feeder in feeders:
        customers = feeder.customers
        if not customers:
            continue

        imp_norm = _normaliser(c.tx_impedance_proxy for c in customers)
        volt_norm = _normaliser(abs(c.tx_est_voltage_pct - 100.0) for c in customers)
        load_norm = _normaliser(c.tx_load_util_pct for c in customers)

        ranked: List[Tuple[float, CustomerAnalytics]] = []
        for c in customers:
            rng = _rng(seed, "uc6", feeder.ref.feeder_mrid, c.mrid)
            risk = (
                0.50 * imp_norm.scale(c.tx_impedance_proxy)
                + 0.30 * volt_norm.scale(abs(c.tx_est_voltage_pct - 100.0))
                + 0.17 * load_norm.scale(c.tx_load_util_pct)
                + 0.03 * (1.0 if c.has_pv else 0.0)
                + rng.uniform(-0.05, 0.06)
            )
            ranked.append((risk, c))

        ranked.sort(key=lambda x: x[0], reverse=True)
        if len(customers) <= 2:
            target_count = len(customers)
        else:
            base_count = 2 + int(round(len(customers) / 350.0))
            target_count = int(_clamp(base_count, 2, 5))
            target_count = min(target_count, len(customers))

        min_sep_km = 0.25 if len(customers) < 180 else 0.35
        selected: List[Tuple[float, CustomerAnalytics]] = []
        selected_ids: Set[str] = set()

        for risk, c in ranked:
            if all(_distance_km(c.lon, c.lat, s.lon, s.lat) >= min_sep_km for _, s in selected):
                selected.append((risk, c))
                selected_ids.add(c.mrid)
                if len(selected) >= target_count:
                    break

        if len(selected) < target_count:
            for risk, c in ranked:
                if c.mrid in selected_ids:
                    continue
                selected.append((risk, c))
                selected_ids.add(c.mrid)
                if len(selected) >= target_count:
                    break

        for risk, c in selected:
            rng = _rng(seed, "uc6-detected", feeder.ref.feeder_mrid, c.mrid)
            fault_events = int(round(_clamp(1 + risk * 4 + rng.uniform(0, 1.0), 1, 6)))
            confidence = round(_clamp(62 + risk * 32 + rng.uniform(-3, 3), 55, 98), 1)
            loop_calc = round(max(0.05, c.tx_impedance_proxy * (0.78 + 0.48 * risk)), 3)
            loop_gis = round(loop_calc * (0.92 + rng.uniform(-0.05, 0.16)), 3)
            non_compliance_days = int(round(_clamp(2 + risk * 34 + rng.uniform(0, 6), 1, 65)))

            features.append(
                Feature(
                    id=f"uc6-{c.mrid}",
                    geometry=Point((c.lon, c.lat)),
                    properties={
                        "use_case": 6,
                        "feature_kind": "neutral_customer",
                        "zone_code": c.zone_code,
                        "feeder_mrid": c.feeder_mrid,
                        "feeder_name": c.feeder_name,
                        "customer_mrid": c.mrid,
                        "fault_events": fault_events,
                        "fault_label": f"{fault_events}",
                        "confidence_pct": confidence,
                        "loop_impedance_calc_ohm": loop_calc,
                        "loop_impedance_gis_ohm": loop_gis,
                        "non_compliance_days": non_compliance_days,
                        "period_start": period_start,
                        "period_end": period_end,
                        "dashboard_key": f"neutral-integrity::{c.zone_code}::{c.feeder_mrid}::{c.mrid}",
                    },
                )
            )

    return FeatureCollection(features)


def _build_uc7_voltage_reporting(feeders: Sequence[FeederAnalytics], seed: int, period_start: str, period_end: str) -> FeatureCollection:
    all_customers = [c for feeder in feeders for c in feeder.customers]
    imp_norm = _normaliser(c.tx_impedance_proxy for c in all_customers)
    volt_norm = _normaliser(abs(c.tx_est_voltage_pct - 100.0) for c in all_customers)

    features: List[Feature] = []
    for c in all_customers:
        rng = _rng(seed, "uc7", c.mrid)
        voltage_dev = abs(c.tx_est_voltage_pct - 100.0)
        heat = _clamp(
            0.08
            + 0.62 * volt_norm.scale(voltage_dev)
            + 0.23 * imp_norm.scale(c.tx_impedance_proxy)
            + 0.07 * (1.0 if c.has_pv else 0.0)
            + rng.uniform(-0.04, 0.06),
            0.02,
            1.0,
        )
        severe = 1 if heat >= 0.72 else 0

        features.append(
            Feature(
                id=f"uc7-{c.mrid}",
                geometry=Point((c.lon, c.lat)),
                properties={
                    "use_case": 7,
                    "feature_kind": "voltage_customer",
                    "zone_code": c.zone_code,
                    "feeder_mrid": c.feeder_mrid,
                    "feeder_name": c.feeder_name,
                    "customer_mrid": c.mrid,
                    "voltage_heat": round(heat, 3),
                    "voltage_heat_weight": round(heat, 3),
                    "voltage_dev_pct": round(voltage_dev, 3),
                    "tx_est_voltage_pct": round(c.tx_est_voltage_pct, 3),
                    "severe": severe,
                    "period_start": period_start,
                    "period_end": period_end,
                    "dashboard_key": f"voltage-monitoring::{c.zone_code}::{c.feeder_mrid}::{c.mrid}",
                },
            )
        )

    return FeatureCollection(features)


def _build_uc8_dynamic_voltage_control(feeders: Sequence[FeederAnalytics], period_start: str, period_end: str) -> FeatureCollection:
    features: List[Feature] = []

    for feeder in feeders:
        for line in feeder.mv_lines:
            features.append(
                Feature(
                    id=f"uc8-line-{line.mrid}",
                    geometry=LineString(line.coordinates),
                    properties={
                        "use_case": 8,
                        "feature_kind": "dvc_mv_line",
                        "zone_code": line.zone_code,
                        "feeder_mrid": line.feeder_mrid,
                        "feeder_name": line.feeder_name,
                        "line_mrid": line.mrid,
                        "line_name": line.name,
                        "base_kv": round(line.base_kv, 3),
                        "length_km": round(line.length_km, 3),
                        "z_ohm_per_km": round(line.z_ohm_per_km, 3),
                        "admd_kw": round(line.downstream_ec_count * ADMD_KW_PER_CUSTOMER, 3),
                        "admg_kw": round(line.downstream_pv_count * ADMG_KW_PER_CUSTOMER, 3),
                        "net_kw": round(line.net_kw, 3),
                        "signed_voltage_change_pct": round(line.signed_voltage_change_pct, 4),
                        "estimated_end_voltage_pct": round(line.estimated_end_voltage_pct, 4),
                        "tap_zone": line.tap_zone,
                        "period_start": period_start,
                        "period_end": period_end,
                        "dashboard_key": f"dynamic-voltage-control::{line.zone_code}::{line.feeder_mrid}::{line.mrid}",
                    },
                )
            )

        for tx in feeder.transformers:
            if tx.lon is None or tx.lat is None or not tx.tap_change_required:
                continue
            features.append(
                Feature(
                    id=f"uc8-tx-{tx.mrid}",
                    geometry=Point((tx.lon, tx.lat)),
                    properties={
                        "use_case": 8,
                        "feature_kind": "dvc_tap_transformer",
                        "zone_code": tx.zone_code,
                        "feeder_mrid": tx.feeder_mrid,
                        "feeder_name": tx.feeder_name,
                        "transformer_mrid": tx.mrid,
                        "transformer_name": tx.name,
                        "estimated_mv_voltage_pct": round(tx.estimated_mv_voltage_pct, 4),
                        "tap_from": tx.tap_from,
                        "tap_to": tx.tap_to,
                        "tap_move": f"{tx.tap_from}->{tx.tap_to}",
                        "tap_action": tx.tap_action,
                        "period_start": period_start,
                        "period_end": period_end,
                        "dashboard_key": f"dynamic-voltage-control::{tx.zone_code}::{tx.feeder_mrid}::{tx.mrid}",
                    },
                )
            )

    return FeatureCollection(features)


def _build_uc9_phase_identification(feeders: Sequence[FeederAnalytics], period_start: str, period_end: str) -> FeatureCollection:
    features: List[Feature] = []
    for feeder in feeders:
        for c in feeder.customers:
            features.append(
                Feature(
                    id=f"uc9-{c.mrid}",
                    geometry=Point((c.lon, c.lat)),
                    properties={
                        "use_case": 9,
                        "feature_kind": "phase_customer",
                        "zone_code": c.zone_code,
                        "feeder_mrid": c.feeder_mrid,
                        "feeder_name": c.feeder_name,
                        "customer_mrid": c.mrid,
                        "phase_class": c.phase_class,
                        "period_start": period_start,
                        "period_end": period_end,
                        "dashboard_key": f"phase-identification::{c.zone_code}::{c.feeder_mrid}::{c.mrid}",
                    },
                )
            )
    return FeatureCollection(features)


def _build_uc13_cer_compliance(feeders: Sequence[FeederAnalytics], seed: int, period_start: str, period_end: str) -> FeatureCollection:
    all_customers = [c for feeder in feeders for c in feeder.customers]
    imp_norm = _normaliser(c.tx_impedance_proxy for c in all_customers)

    features: List[Feature] = []
    for c in all_customers:
        if not c.has_pv:
            continue

        rng = _rng(seed, "uc13", c.mrid)
        voltage_risk = _clamp(abs(c.tx_est_voltage_pct - 100.0) / 2.5, 0.0, 1.0)
        gen_risk = _clamp(c.tx_generation_util_pct / 130.0, 0.0, 1.0)
        impedance_risk = imp_norm.scale(c.tx_impedance_proxy)
        risk = _clamp(0.45 * voltage_risk + 0.35 * gen_risk + 0.20 * impedance_risk + rng.uniform(-0.08, 0.08), 0.0, 1.0)

        if risk >= 0.72:
            tier = "Non-Compliant"
            issue = "Over-export risk"
        elif risk >= 0.42:
            tier = "At Risk"
            issue = "Potential Volt/Watt concern"
        else:
            tier = "Compliant"
            issue = "No active issue"

        unresolved_days = int(round(_clamp(risk * 90 + rng.uniform(0, 12), 0, 120)))

        features.append(
            Feature(
                id=f"uc13-{c.mrid}",
                geometry=Point((c.lon, c.lat)),
                properties={
                    "use_case": 13,
                    "feature_kind": "cer_compliance_customer",
                    "zone_code": c.zone_code,
                    "feeder_mrid": c.feeder_mrid,
                    "feeder_name": c.feeder_name,
                    "customer_mrid": c.mrid,
                    "compliance_tier": tier,
                    "compliance_issue": issue,
                    "unresolved_days": unresolved_days,
                    "risk_score": round(risk * 100.0, 1),
                    "period_start": period_start,
                    "period_end": period_end,
                    "dashboard_key": f"cer-compliance::{c.zone_code}::{c.feeder_mrid}::{c.mrid}",
                },
            )
        )

    return FeatureCollection(features)


def _build_uc14_cer_performance(feeders: Sequence[FeederAnalytics], seed: int, period_start: str, period_end: str) -> FeatureCollection:
    features: List[Feature] = []

    for feeder in feeders:
        for tx in feeder.transformers:
            if tx.lon is None or tx.lat is None:
                continue

            rng = _rng(seed, "uc14", tx.mrid)
            pec_pct = tx.pec_penetration_pct_of_rating
            curtailment_kwh = max(0.0, tx.curtailment_kwh * (0.9 + rng.uniform(-0.05, 0.18)))
            value_aud = curtailment_kwh * (0.08 + rng.uniform(-0.01, 0.04))

            features.append(
                Feature(
                    id=f"uc14-{tx.mrid}",
                    geometry=Point((tx.lon, tx.lat)),
                    properties={
                        "use_case": 14,
                        "feature_kind": "cer_performance_transformer",
                        "zone_code": tx.zone_code,
                        "feeder_mrid": tx.feeder_mrid,
                        "feeder_name": tx.feeder_name,
                        "transformer_mrid": tx.mrid,
                        "transformer_name": tx.name,
                        "pec_percent_rating": round(pec_pct, 2),
                        "curtailed_kwh": round(curtailment_kwh, 2),
                        "curtailment_value_aud": round(value_aud, 2),
                        "curtailment_label": f"{int(round(curtailment_kwh))}kWh",
                        "period_start": period_start,
                        "period_end": period_end,
                        "dashboard_key": f"cer-performance::{tx.zone_code}::{tx.feeder_mrid}::{tx.mrid}",
                    },
                )
            )

    return FeatureCollection(features)


def _build_uc15_ev_detection(feeders: Sequence[FeederAnalytics], seed: int, period_start: str, period_end: str) -> FeatureCollection:
    features: List[Feature] = []

    for feeder in feeders:
        customers = feeder.customers
        if not customers:
            continue

        feeder_rng = _rng(seed, "uc15-feeder", feeder.ref.feeder_mrid)
        # Keep average EV detection no more than 1-in-8 per feeder.
        target_ratio = _clamp(0.09 + feeder_rng.uniform(-0.015, 0.02), 0.07, 0.125)
        target_count = int(round(len(customers) * target_ratio))
        if target_count <= 0 and len(customers) >= 32:
            target_count = 1

        pocket_count = min(4, max(1, int(round(math.sqrt(len(customers)) / 40.0)) + 1))
        pocket_centres = feeder_rng.sample(customers, min(pocket_count, len(customers)))

        ranked: List[Tuple[float, CustomerAnalytics]] = []
        for c in customers:
            random_component = feeder_rng.random()
            pocket_boost = 0.0
            for centre in pocket_centres:
                d_km = _distance_km(c.lon, c.lat, centre.lon, centre.lat)
                pocket_boost = max(pocket_boost, math.exp(-(d_km * d_km) / 0.06))

            score = 0.52 * random_component + 0.48 * pocket_boost + feeder_rng.uniform(-0.06, 0.08)
            ranked.append((score, c))

        ranked.sort(key=lambda x: x[0], reverse=True)
        selected = ranked[:target_count]

        for _, c in selected:
            rng = _rng(seed, "uc15", c.mrid)
            charger_type = "L1" if rng.random() < 0.3 else "L2"
            if charger_type == "L1":
                peak_kw = round(rng.uniform(2.4, 6.9), 1)
            else:
                if rng.random() < 0.22:
                    peak_kw = round(rng.uniform(11.0, 22.0), 1)
                else:
                    peak_kw = round(rng.uniform(7.0, 11.5), 1)

            features.append(
                Feature(
                    id=f"uc15-{c.mrid}",
                    geometry=Point((c.lon, c.lat)),
                    properties={
                        "use_case": 15,
                        "feature_kind": "ev_customer",
                        "zone_code": c.zone_code,
                        "feeder_mrid": c.feeder_mrid,
                        "feeder_name": c.feeder_name,
                        "customer_mrid": c.mrid,
                        "ev_detected": True,
                        "charger_type": charger_type,
                        "peak_kw": peak_kw,
                        "ev_label": f"{charger_type} {peak_kw}kW",
                        "period_start": period_start,
                        "period_end": period_end,
                        "dashboard_key": f"ev-detection::{c.zone_code}::{c.feeder_mrid}::{c.mrid}",
                    },
                )
            )

    return FeatureCollection(features)


def _build_results(feeders: Sequence[FeederAnalytics], seed: int, period_start: str, period_end: str) -> List[StudyResultInput]:
    uc6 = _build_uc6_neutral_faults(feeders, seed, period_start, period_end)
    uc7 = _build_uc7_voltage_reporting(feeders, seed, period_start, period_end)
    uc8 = _build_uc8_dynamic_voltage_control(feeders, period_start, period_end)
    uc9 = _build_uc9_phase_identification(feeders, period_start, period_end)
    uc13 = _build_uc13_cer_compliance(feeders, seed, period_start, period_end)
    uc14 = _build_uc14_cer_performance(feeders, seed, period_start, period_end)
    uc15 = _build_uc15_ev_detection(feeders, seed, period_start, period_end)

    return [
        StudyResultInput(
            name=USE_CASE_NAMES[6],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc6, styles=["uc6-neutral-customer", "uc6-neutral-label"]),
        ),
        StudyResultInput(
            name=USE_CASE_NAMES[7],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc7, styles=["uc7-voltage-heatmap", "uc7-voltage-hotspot"]),
        ),
        StudyResultInput(
            name=USE_CASE_NAMES[8],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc8, styles=["uc8-dvc-mv-line", "uc8-dvc-tap-transformer", "uc8-dvc-tap-label"]),
        ),
        StudyResultInput(
            name=USE_CASE_NAMES[9],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc9, styles=["uc9-phase-customer"]),
        ),
        StudyResultInput(
            name=USE_CASE_NAMES[13],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc13, styles=["uc13-compliance-customer"]),
        ),
        StudyResultInput(
            name=USE_CASE_NAMES[14],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc14, styles=["uc14-curtailment-transformer", "uc14-curtailment-label"]),
        ),
        StudyResultInput(
            name=USE_CASE_NAMES[15],
            sections=[],
            geo_json_overlay=GeoJsonOverlayInput(data=uc15, styles=["uc15-ev-customer", "uc15-ev-label"]),
        ),
    ]


def _count_features(results: Sequence[StudyResultInput]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for result in results:
        data = result.geo_json_overlay.data
        counts[result.name] = len(data.features) if hasattr(data, "features") else 0
    return counts


async def main(argv: Sequence[str]) -> None:
    args = parse_args(argv)
    zone_codes = [z.strip() for z in args.zones.split(",") if z.strip()]
    if not zone_codes:
        raise ValueError("At least one zone must be provided via --zones")

    config = _load_config(args.config)
    styles = _load_styles()

    feeder_refs = await _collect_feeders(config, zone_codes)
    if not feeder_refs:
        raise ValueError(f"No feeders found in zones: {', '.join(zone_codes)}")

    print(f"Analysing {len(feeder_refs)} feeders from zones: {', '.join(zone_codes)}")

    feeder_analytics: List[FeederAnalytics] = []
    for batch in _chunk(feeder_refs, args.batch_size):
        tasks = [asyncio.create_task(_analyse_feeder(ref, config, args.seed)) for ref in batch]
        for result in await asyncio.gather(*tasks):
            if result is not None:
                feeder_analytics.append(result)

    if not feeder_analytics:
        raise RuntimeError("No feeder analytics generated. Check network data/locations.")

    feeder_analytics.sort(key=lambda f: (f.ref.zone_code, f.ref.feeder_name))

    results = _build_results(
        feeders=feeder_analytics,
        seed=args.seed,
        period_start=args.period_start,
        period_end=args.period_end,
    )

    print("Generated demo analytics layers:")
    for layer_name, count in _count_features(results).items():
        print(f"  - {layer_name}: {count} feature(s)")

    if args.dry_run:
        print("Dry-run enabled. Study was not uploaded.")
        return

    eas_client = create_eas_client_from_config(config)

    study = StudyInput(
        name=args.name,
        description=(
            "Zone-substation analytics demo generated from live feeder topology with correlated synthetic "
            "network-performance indicators. DVC assumes ADMD=4kW and ADMG=5kW per customer."
        ),
        tags=["analytics_demo", "zone_substations", "regional_overview", "correlated_data"],
        results=results,
        styles=styles,
    )

    print(f"Uploading study for zones: {', '.join(zone_codes)}")
    response = await eas_client.mutation(Mutation.add_studies(studies=[study]))
    await eas_client.close()
    print(f"Study upload response: {response}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
