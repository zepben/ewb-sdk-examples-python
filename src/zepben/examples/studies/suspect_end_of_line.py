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
from zepben.ewb import PowerTransformer, ConductingEquipment, EnergyConsumer, AcLineSegment, Switch, \
    NetworkConsumerClient, PhaseCode, PowerElectronicsConnection, Feeder, PowerSystemResource, Location, \
    connect_with_token, NetworkTraceStep, Tracing, downstream, upstream, IncludedEnergizedContainers


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
    # Extract all the feeders to process
    feeder_mrids = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)

    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")

    # Process the feeders in batches of 3, using asyncio, for performance
    batches = chunk(feeder_mrids, 3)
    for feeders in batches:
        feeder_results = []
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
            futures.append(asyncio.ensure_future(fetch_feeder_and_trace(feeder_mrid, rpc_channel)))

        for future in futures:
            result = await future
            if not result:   # Empty if the feeder failed
                continue
            feeder_mrid, transformers, tx_to_sus_lines, feeder_suspect_lines = result
            total_length_m = sum(_line_length_m(line) for line in feeder_suspect_lines)
            highlight_equipment = set(transformers) | set(feeder_suspect_lines)
            feeder_results.append(
                (feeder_mrid, total_length_m, list(highlight_equipment), tx_to_sus_lines)
            )

        print(f"Created Study for {len(feeder_mrids)} feeders")

        eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"], asynchronous=True, enable_legacy_methods=True)

        print(f"Uploading Study for {', '.join(feeders)} ...")
        styles = json.load(open("style_eol.json", "r"))
        results = [
            _build_suspect_end_result(
                feeder_mrid,
                total_length_m,
                equipment,
                transformer_to_suspect_lines,
                styles=styles,
            )
            for feeder_mrid, total_length_m, equipment, transformer_to_suspect_lines in feeder_results
        ]
        await upload_suspect_end_of_line_study(
            eas_client,
            results,
            name=f"Suspect end of line ({', '.join(feeders)})",
            description="Highlights only line segments that have no downstream EnergyConsumers (excludes shared upstream segments).",
            tags=["suspect_end_of_line", "-".join(zone_mrids)],
            styles=styles
        )
        await eas_client.close()
        print(f"Uploaded Study")

    print(f"Finish time: {datetime.now()}")


def collect_downstream_edges_provider(
    adjacency: Dict[ConductingEquipment, Set[ConductingEquipment]],
    nodes: Set[ConductingEquipment],
):

    async def collect_edges(ps: NetworkTraceStep, _):
        nodes.add(ps.path.from_equipment)
        nodes.add(ps.path.to_equipment)
        if ps.path.traced_externally:
            adjacency.setdefault(ps.path.from_equipment, set()).add(ps.path.to_equipment)

    return collect_edges


async def build_downstream_graph(
    start: ConductingEquipment,
) -> Tuple[Dict[ConductingEquipment, Set[ConductingEquipment]], Set[ConductingEquipment]]:
    adjacency: Dict[ConductingEquipment, Set[ConductingEquipment]] = {}
    nodes: Set[ConductingEquipment] = {start}

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(collect_downstream_edges_provider(adjacency, nodes))
    ).run(start=start, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    for node in nodes:
        adjacency.setdefault(node, set())

    return adjacency, nodes


def build_has_consumer_downstream(
    adjacency: Dict[ConductingEquipment, Set[ConductingEquipment]],
    backfeed_switches: Set[ConductingEquipment],
):
    memo: Dict[ConductingEquipment, bool] = {}
    visiting: Set[ConductingEquipment] = set()

    def has_consumer(node: ConductingEquipment) -> bool:
        if node in memo:
            return memo[node]
        if node in visiting:
            return False
        visiting.add(node)
        result = isinstance(node, EnergyConsumer) or node in backfeed_switches
        if not result:
            for child in adjacency.get(node, ()):
                if has_consumer(child):
                    result = True
                    break
        visiting.remove(node)
        memo[node] = result
        return result

    return has_consumer


async def fetch_feeder_and_trace(feeder_mrid: str, rpc_channel):
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)

    result = (
        await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS
        )
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return None

    network = client.service

    print(f"Finished fetching Feeder {feeder_mrid}")
    print(f"Tracing suspect lines for feeder {feeder_mrid}")
    transformers: List[PowerTransformer] = []
    transformer_to_suspect_lines: Dict[str, Tuple[float, List[ConductingEquipment]]] = {}
    feeder_suspect_lines: Set[AcLineSegment] = set()
    for io in (pt for pt in network.objects(PowerTransformer)):
        pt: PowerTransformer = io
        transformers.append(pt)
        suspect_lines = await get_suspect_lines_for_transformer(pt)
        feeder_suspect_lines.update(suspect_lines)
        total_length_m = sum(_line_length_m(line) for line in suspect_lines)
        transformer_to_suspect_lines[pt.mrid] = (total_length_m, list(suspect_lines))

    return feeder_mrid, transformers, transformer_to_suspect_lines, feeder_suspect_lines


def _switch_has_external_network(
    switch: Switch,
    nodes: Set[ConductingEquipment],
) -> bool:
    for terminal in switch.terminals:
        cn = terminal.connectivity_node
        if cn is None:
            continue
        for term in cn.terminals:
            ce = term.conducting_equipment
            if ce is None or ce is switch:
                continue
            if ce not in nodes:
                return True
    return False


def _find_backfeed_switches(
    nodes: Set[ConductingEquipment],
) -> Set[ConductingEquipment]:
    backfeed = set()
    for node in nodes:
        if isinstance(node, Switch) and _switch_has_external_network(node, nodes):
            backfeed.add(node)
    return backfeed


async def get_suspect_lines_for_transformer(
    transformer: PowerTransformer,
) -> Set[AcLineSegment]:
    adjacency, nodes = await build_downstream_graph(transformer)
    backfeed_switches = _find_backfeed_switches(nodes)
    has_consumer = build_has_consumer_downstream(adjacency, backfeed_switches)

    suspect_lines = {
        node for node in nodes
        if isinstance(node, AcLineSegment) and not has_consumer(node)
    }
    return suspect_lines


def _build_suspect_end_result(
    feeder_mrid: str,
    total_length_m: float,
    pts: List[ConductingEquipment],
    transformer_to_suspect_lines: Dict[str, Tuple[float, List[ConductingEquipment]]],
    styles: List
) -> StudyResultInput:
    class_to_properties = {
        EnergyConsumer: {
            "name": lambda ec: ec.name,
            "type": lambda x: "ec"
        },
        PowerTransformer: {
            "suspect_length_m": _suspect_length_m_from(transformer_to_suspect_lines),
            "suspect_length_label": _suspect_length_label_from(transformer_to_suspect_lines),
            "type": lambda x: "pt"
        },
        AcLineSegment: {
            "name": lambda ec: ec.name,
            "length_m": lambda line: _line_length_m(line),
        },
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
    result_name = f"{feeder_mrid} - {round(total_length_m)}m"
    return StudyResultInput(
        name=result_name,
        sections=[],
        geo_json_overlay=GeoJsonOverlayInput(
            data=feature_collection,
            styles=[s['id'] for s in styles]
        )
    )


async def upload_suspect_end_of_line_study(
    eas_client: EasClient,
    results: List[StudyResultInput],
    name: str,
    description: str,
    tags: List[str],
    styles: List
) -> None:
    response = await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=name,
            description=description,
            tags=tags,
            results=results,
            styles=styles
        )
        ]
    )
    )
    print(f"Study response: {response}")


def _suspect_length_m_from(pt_to_sus_end: Dict[str, Tuple[float, List[ConductingEquipment]]]):
    def fun(pt: PowerTransformer):
        value = pt_to_sus_end.get(pt.mrid)
        return round(value[0]) if value else 0

    return fun


def _suspect_length_label_from(pt_to_sus_end: Dict[str, Tuple[float, List[ConductingEquipment]]]):
    def fun(pt: PowerTransformer):
        value = pt_to_sus_end.get(pt.mrid)
        meters = round(value[0]) if value else 0
        return f"{meters}m"

    return fun


def _line_length_m(line: AcLineSegment) -> float:
    return float(line.length or 0.0)


def to_geojson_feature_collection(
    psrs: List[PowerSystemResource],
    class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]]
) -> FeatureCollection:

    features = []
    for psr in psrs:
        properties_map = class_to_properties.get(type(psr))

        if properties_map is not None:
            features.append(to_geojson_feature(psr, properties_map))

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
    points = list(location.points)
    if len(points) > 1:
        return LineString([(point.x_position, point.y_position) for point in points])
    elif len(points) == 1:
        return Point((points[0].x_position, points[0].y_position))
    else:
        return None


if __name__ == "__main__":
    asyncio.run(main())
