#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
from datetime import datetime
from itertools import islice
from typing import List, Dict, Tuple, Callable, Any, Union, Type, Set

from geojson import FeatureCollection, Feature
from geojson.geometry import Geometry, LineString, Point
from zepben.eas import EasClient, Mutation, StudyInput, StudyResultInput, GeoJsonOverlayInput
from zepben.ewb import (
    AcLineSegment,
    EnergyConsumer,
    PowerTransformer,
    NetworkConsumerClient,
    PhaseCode,
    Feeder,
    PowerSystemResource,
    Location,
    connect_with_token,
    NetworkTraceStep,
    Tracing,
    upstream,
    stop_at_open,
    IncludedEnergizedContainers,
)
from zepben.ewb.services.network.tracing.networktrace.operators.network_state_operators import NetworkStateOperators


with open("../config.json") as f:
    c = json.loads(f.read())


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


async def main():
    # Only process feeders in the following zones
    zone_mrids = ["CPM"]
    print(f"Start time: {datetime.now()}")

    rpc_channel = connect_with_token(
        host=c["host"],
        access_token=c["access_token"],
        rpc_port=c["rpc_port"],
        ca_filename=c.get("ca_filename"),
        timeout_seconds=c.get("timeout_seconds", 5),
        debug=bool(c.get("debug", False)),
        skip_connection_test=bool(c.get("skip_connection_test", False)),
    )
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()
    substations = hierarchy.value.substations

    print(f"Collecting feeders from zones {', '.join(zone_mrids)}.")
    feeder_mrids = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)

    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")

    all_ecs: List[EnergyConsumer] = []
    all_lines: List[AcLineSegment] = []
    ec_to_loop_z_phase_phase: Dict[str, float] = {}
    ec_to_loop_z_phase_earth: Dict[str, float] = {}
    line_to_z_per_km: Dict[str, float] = {}

    # Process feeders in batches of 3, using asyncio, for performance
    batches = chunk(feeder_mrids, 3)
    for feeders in batches:
        futures = []
        rpc_channel = connect_with_token(
            host=c["host"],
            access_token=c["access_token"],
            rpc_port=c["rpc_port"],
            ca_filename=c.get("ca_filename"),
            timeout_seconds=c.get("timeout_seconds", 5),
            debug=bool(c.get("debug", False)),
            skip_connection_test=bool(c.get("skip_connection_test", False)),
        )
        print(f"Processing feeders {', '.join(feeders)}")
        for feeder_mrid in feeders:
            futures.append(asyncio.ensure_future(fetch_feeder_loop_impedance(feeder_mrid, rpc_channel)))

        for future in futures:
            result = await future
            if result is None:
                continue
            ecs, lines, ec_impedance_phase_phase, ec_impedance_phase_earth, line_impedance = result
            all_ecs.extend(ecs)
            all_lines.extend(lines)
            ec_to_loop_z_phase_phase.update(ec_impedance_phase_phase)
            ec_to_loop_z_phase_earth.update(ec_impedance_phase_earth)
            line_to_z_per_km.update(line_impedance)

    print(f"Creating study for {len(all_ecs)} energy consumers")

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"], asynchronous=True, enable_legacy_methods=True)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await upload_loop_impedance_study(
        eas_client,
        all_ecs,
        all_lines,
        ec_to_loop_z_phase_phase,
        ec_to_loop_z_phase_earth,
        line_to_z_per_km,
        name=f"Loop impedance (normal) ({', '.join(zone_mrids)})",
        description=(
            "Loop impedance approximations at EnergyConsumers on the normal upstream network path to transformer; "
            "AC line segments are colored by positive-sequence impedance."
        ),
        tags=["loop_impedance", "-".join(zone_mrids)],
        styles=json.load(open("style_loop_impedance.json", "r")),
    )
    await eas_client.close()
    print("Uploaded Study")

    print(f"Finish time: {datetime.now()}")


async def fetch_feeder_loop_impedance(
    feeder_mrid: str,
    rpc_channel,
) -> Union[Tuple[List[EnergyConsumer], List[AcLineSegment], Dict[str, float], Dict[str, float], Dict[str, float]], None]:
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)

    result = (
        await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
        )
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return None

    network = client.service
    print(f"Finished fetching Feeder {feeder_mrid}")

    # Required for directed traces (upstream/downstream)
    await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)

    ecs = list(network.objects(EnergyConsumer))
    lines = list(network.objects(AcLineSegment))

    line_to_z_per_km = {
        line.mrid: _line_impedance_per_km(line)
        for line in lines
    }

    ec_to_loop_z_phase_phase: Dict[str, float] = {}
    ec_to_loop_z_phase_earth: Dict[str, float] = {}
    for ec in ecs:
        path_lines = await get_upstream_lines_to_transformer(ec)
        z1_path, z2_path, z0_path = _path_sequence_impedance(path_lines)
        ec_to_loop_z_phase_phase[ec.mrid] = abs(2.0 * z1_path)
        ec_to_loop_z_phase_earth[ec.mrid] = abs(z1_path + z2_path + z0_path)

    return ecs, lines, ec_to_loop_z_phase_phase, ec_to_loop_z_phase_earth, line_to_z_per_km


def _line_impedance_per_m(line: AcLineSegment) -> float:
    z1 = _line_z1_per_m(line)
    return abs(z1)


def _line_z1_per_m(line: AcLineSegment) -> complex:
    plsi = line.per_length_sequence_impedance
    if plsi is None:
        return 0j
    r = plsi.r or 0.0
    x = plsi.x or 0.0
    return complex(r, x)


def _line_z2_per_m(line: AcLineSegment) -> complex:
    plsi = line.per_length_sequence_impedance
    if plsi is None:
        return 0j
    r2 = getattr(plsi, "r2", None)
    x2 = getattr(plsi, "x2", None)
    if r2 is None:
        r2 = plsi.r
    if x2 is None:
        x2 = plsi.x
    return complex(r2 or 0.0, x2 or 0.0)


def _line_z0_per_m(line: AcLineSegment) -> complex:
    plsi = line.per_length_sequence_impedance
    if plsi is None:
        return 0j
    r0 = getattr(plsi, "r0", 0.0)
    x0 = getattr(plsi, "x0", 0.0)
    return complex(r0 or 0.0, x0 or 0.0)


def _path_sequence_impedance(path_lines: Set[AcLineSegment]) -> Tuple[complex, complex, complex]:
    z1_total = 0j
    z2_total = 0j
    z0_total = 0j
    for line in path_lines:
        # EWB AcLineSegment.length is in metres.
        length_m = line.length or 0.0
        z1_total += _line_z1_per_m(line) * length_m
        z2_total += _line_z2_per_m(line) * length_m
        z0_total += _line_z0_per_m(line) * length_m
    return z1_total, z2_total, z0_total


def _line_impedance_per_km(line: AcLineSegment) -> float:
    # Convert per-metre magnitude to per-km for line styling/legend display.
    return _line_impedance_per_m(line) * 1000.0


def collect_upstream_lines_provider(lines: Set[AcLineSegment]):

    async def collect_lines(ps: NetworkTraceStep, _):
        line = ps.path.traversed_ac_line_segment
        if line is not None:
            lines.add(line)
        if isinstance(ps.path.to_equipment, AcLineSegment):
            lines.add(ps.path.to_equipment)
        if isinstance(ps.path.from_equipment, AcLineSegment):
            lines.add(ps.path.from_equipment)

    return collect_lines


async def get_upstream_lines_to_transformer(ec: EnergyConsumer) -> Set[AcLineSegment]:
    lines = set()

    await (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_condition(stop_at_open())
        .add_step_action(collect_upstream_lines_provider(lines))
        .add_stop_condition(_is_transformer)
    ).run(start=ec, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return lines


def _is_transformer(ps: NetworkTraceStep, _context=None) -> bool:
    return isinstance(ps.path.to_equipment, PowerTransformer)


async def upload_loop_impedance_study(
    eas_client: EasClient,
    ecs: List[EnergyConsumer],
    lines: List[AcLineSegment],
    ec_to_loop_z_phase_phase: Dict[str, float],
    ec_to_loop_z_phase_earth: Dict[str, float],
    line_to_z_per_km: Dict[str, float],
    name: str,
    description: str,
    tags: List[str],
    styles: List,
) -> None:

    class_to_properties = {
        EnergyConsumer: {
            "name": lambda ec: ec.name,
            "type": lambda x: "ec",
        },
        AcLineSegment: {
            "name": lambda line: line.name,
            "z_ohm_per_km": _line_z_per_km_from(line_to_z_per_km),
            "type": lambda x: "line",
        },
    }
    phase_phase_feature_collection = _to_loop_feature_collection(ecs, lines, class_to_properties, ec_to_loop_z_phase_phase)
    phase_earth_feature_collection = _to_loop_feature_collection(ecs, lines, class_to_properties, ec_to_loop_z_phase_earth)
    response = await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=name,
            description=description,
            tags=tags,
            results=[
                StudyResultInput(
                    name="Phase-to-Phase Loop Approximation",
                    sections=[],
                    geo_json_overlay=GeoJsonOverlayInput(
                        data=phase_phase_feature_collection,
                        styles=[s['id'] for s in styles]
                    )
                ),
                StudyResultInput(
                    name="Phase-to-Earth Loop Approximation",
                    sections=[],
                    geo_json_overlay=GeoJsonOverlayInput(
                        data=phase_earth_feature_collection,
                        styles=[s['id'] for s in styles]
                    )
                )
            ],
            styles=styles
        )
        ]
    )
    )
    print(f"Study response: {response}")


def _loop_z_from(ec_to_loop_z: Dict[str, float]):
    def fun(ec: EnergyConsumer):
        return round(ec_to_loop_z.get(ec.mrid, 0.0), 4)

    return fun


def _loop_z_label_from(ec_to_loop_z: Dict[str, float]):
    def fun(ec: EnergyConsumer):
        value = ec_to_loop_z.get(ec.mrid, 0.0)
        return f"{value:.2f}Ω"

    return fun


def _line_z_per_km_from(line_to_z_per_km: Dict[str, float]):
    def fun(line: AcLineSegment):
        return round(line_to_z_per_km.get(line.mrid, 0.0), 4)

    return fun


def to_geojson_feature_collection(
    psrs: List[PowerSystemResource],
    class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]]
) -> FeatureCollection:

    features = []
    for psr in psrs:
        properties_map = class_to_properties.get(type(psr))

        if properties_map is not None:
            feature = to_geojson_feature(psr, properties_map)
            if feature is not None:
                features.append(feature)

    return FeatureCollection(features)


def _to_loop_feature_collection(
    ecs: List[EnergyConsumer],
    lines: List[AcLineSegment],
    class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]],
    ec_to_loop_z: Dict[str, float],
) -> FeatureCollection:
    properties = {
        **class_to_properties,
        EnergyConsumer: {
            **class_to_properties[EnergyConsumer],
            "loop_z_ohm": _loop_z_from(ec_to_loop_z),
            "loop_z_label": _loop_z_label_from(ec_to_loop_z),
        },
    }
    return to_geojson_feature_collection(ecs + lines, properties)


def to_geojson_feature(
    psr: PowerSystemResource,
    property_map: Dict[str, Callable[[PowerSystemResource], Any]]
) -> Union[Feature, None]:

    geometry = to_geojson_geometry(psr.location)
    if geometry is None:
        return None

    properties = {k: f(psr) for (k, f) in property_map.items()}
    return Feature(psr.mrid, geometry, properties)


def to_geojson_geometry(location: Location) -> Union[Geometry, None]:
    points = list(location.points) if location is not None else []
    if len(points) > 1:
        return LineString([(point.x_position, point.y_position) for point in points])
    elif len(points) == 1:
        return Point((points[0].x_position, points[0].y_position))
    else:
        return None


if __name__ == "__main__":
    asyncio.run(main())
