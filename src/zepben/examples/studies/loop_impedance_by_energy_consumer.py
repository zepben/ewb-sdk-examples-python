#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import math
from datetime import datetime
from itertools import islice
from typing import List, Dict, Tuple, Callable, Any, Union, Type, Set

from geojson import FeatureCollection, Feature
from geojson.geometry import Geometry, LineString, Point
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import Study, Result, GeoJsonOverlay
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
    ec_to_loop_z: Dict[str, float] = {}
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
            ecs, lines, ec_impedance, line_impedance = result
            all_ecs.extend(ecs)
            all_lines.extend(lines)
            ec_to_loop_z.update(ec_impedance)
            line_to_z_per_km.update(line_impedance)

    print(f"Creating study for {len(all_ecs)} energy consumers")

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await upload_loop_impedance_study(
        eas_client,
        all_ecs,
        all_lines,
        ec_to_loop_z,
        line_to_z_per_km,
        name=f"Loop impedance (normal) ({', '.join(zone_mrids)})",
        description="Loop impedance at EnergyConsumers on normal network path; AC line segments colored by impedance.",
        tags=["loop_impedance", "-".join(zone_mrids)],
        styles=json.load(open("style_loop_impedance.json", "r")),
    )
    await eas_client.aclose()
    print("Uploaded Study")

    print(f"Finish time: {datetime.now()}")


async def fetch_feeder_loop_impedance(
    feeder_mrid: str,
    rpc_channel,
) -> Union[Tuple[List[EnergyConsumer], List[AcLineSegment], Dict[str, float], Dict[str, float]], None]:
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

    ec_to_loop_z = {}
    for ec in ecs:
        path_lines = await get_upstream_lines_to_transformer(ec)
        loop_z = 2.0 * sum(_line_impedance_per_m(line) * (line.length or 0.0) for line in path_lines)
        ec_to_loop_z[ec.mrid] = loop_z

    return ecs, lines, ec_to_loop_z, line_to_z_per_km


def _line_impedance_per_m(line: AcLineSegment) -> float:
    plsi = line.per_length_sequence_impedance
    if plsi is None:
        return 0.0
    r = plsi.r or 0.0
    x = plsi.x or 0.0
    return math.hypot(r, x)


def _line_impedance_per_km(line: AcLineSegment) -> float:
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
    ec_to_loop_z: Dict[str, float],
    line_to_z_per_km: Dict[str, float],
    name: str,
    description: str,
    tags: List[str],
    styles: List,
) -> None:

    class_to_properties = {
        EnergyConsumer: {
            "name": lambda ec: ec.name,
            "loop_z_ohm": _loop_z_from(ec_to_loop_z),
            "loop_z_label": _loop_z_label_from(ec_to_loop_z),
            "type": lambda x: "ec",
        },
        AcLineSegment: {
            "name": lambda line: line.name,
            "z_ohm_per_km": _line_z_per_km_from(line_to_z_per_km),
            "type": lambda x: "line",
        },
    }
    feature_collection = to_geojson_feature_collection(ecs + lines, class_to_properties)
    response = await eas_client.async_upload_study(
        Study(
            name=name,
            description=description,
            tags=tags,
            results=[
                Result(
                    name=name,
                    geo_json_overlay=GeoJsonOverlay(
                        data=feature_collection,
                        styles=[s['id'] for s in styles]
                    )
                )
            ],
            styles=styles
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
