#  Copyright 2022 Zeppelin Bend Pty Ltd
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
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import Study, Result, GeoJsonOverlay
from zepben.evolve import PowerTransformer, ConductingEquipment, EnergyConsumer, AcLineSegment, \
    NetworkConsumerClient, normal_upstream_trace, PhaseStep, PhaseCode, PowerElectronicsConnection, Feeder, PowerSystemResource, Location, \
    normal_downstream_trace, connect_with_token
from zepben.evolve.services.network.tracing.phases.phase_step import start_at
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


with open("../config.json") as f:
    c = json.loads(f.read())


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


async def main():
    # Only process feeders in the following zones
    zone_mrids = ["MTN"]
    print(f"Start time: {datetime.now()}")

    rpc_channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])
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
    futures = []

    # Process the feeders in batches of 3, using asyncio, for performance
    batches = chunk(feeder_mrids, 3)
    for feeders in batches:
        all_traced_equipment = []
        transformer_to_suspect_end = dict()

        rpc_channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])
        print(f"Processing feeders {', '.join(feeders)}")
        for feeder_mrid in feeders:
            futures.append(asyncio.ensure_future(fetch_feeder_and_trace(feeder_mrid, rpc_channel)))

        for future in futures:
            tx_to_sus_end = await future
            if tx_to_sus_end:   # Empty if the feeder failed
                all_traced_equipment.extend(eq for (k, (count, sus_eq_list)) in tx_to_sus_end.items() for eq in sus_eq_list)
                transformer_to_suspect_end.update(tx_to_sus_end)

        print(f"Created Study for {len(feeder_mrids)} feeders")

        eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])

        print(f"Uploading Study for {', '.join(feeders)} ...")
        await upload_suspect_end_of_line_study(
            eas_client,
            all_traced_equipment,
            transformer_to_suspect_end,
            name=f"Suspect end of line {', '.join(feeders)}",
            description="Highlights every line that is downstream of transformer and ends without a consumer.",
            tags=["suspect_end_of_line", "-".join(zone_mrids)],
            styles=json.load(open("style_eol.json", "r"))
        )
        await eas_client.aclose()
        print(f"Uploaded Study")

    print(f"Finish time: {datetime.now()}")


def collect_eq_provider(collection: Set[ConductingEquipment]):

    async def collect_equipment(ps: PhaseStep, _):
        collection.add(ps.conducting_equipment)

    return collect_equipment


async def get_downstream_eq(ce: ConductingEquipment) -> Set[ConductingEquipment]:
    trace = normal_downstream_trace()
    phase_step = start_at(ce, PhaseCode.ABCN)

    equipment_set = set()
    trace.add_step_action(collect_eq_provider(equipment_set))
    await trace.run(start_item=phase_step, can_stop_on_start_item=False)
    return equipment_set


async def fetch_feeder_and_trace(feeder_mrid: str, rpc_channel):
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)

    result = (await client.get_equipment_container(mrid=feeder_mrid, expected_class=Feeder, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS))
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return {}

    network = client.service

    print(f"Finished fetching Feeder {feeder_mrid}")
    print(f"Tracing downstream transformers for feeder {feeder_mrid}")
    transformer_to_eq: Dict[str, Set[ConductingEquipment]] = {}
    for io in (pt for pt in network.objects(PowerTransformer)):
        pt: PowerTransformer = io
        downstream_equipment = await get_downstream_eq(pt)
        transformer_to_eq[pt.mrid] = downstream_equipment

    print(f"Tracing suspect ends for feeder {feeder_mrid}")
    transformer_to_suspect_end = await get_transformer_to_suspect_end(transformer_to_eq)

    return transformer_to_suspect_end


async def get_transformer_to_suspect_end(transformer_to_eq: Dict[str, Set[ConductingEquipment]]) -> Dict[str, Tuple[int, Set[ConductingEquipment]]]:
    transformer_to_suspect_end: Dict[str, (int, List[ConductingEquipment])] = {}
    for pt_mrid, eq_list in transformer_to_eq.items():
        single_terminal_junctions = [eq for eq in eq_list if not isinstance(eq, (EnergyConsumer, PowerElectronicsConnection)) and len(list(eq.terminals)) == 1]

        upstream_eq = set()
        for stj in single_terminal_junctions:
            upstream_eq_up_to_pt = await _get_upstream_eq_up_to_transformer(stj)
            upstream_eq.update(upstream_eq_up_to_pt)

        transformer_to_suspect_end[pt_mrid] = (len(single_terminal_junctions), list(upstream_eq))

    return transformer_to_suspect_end


async def upload_suspect_end_of_line_study(
    eas_client: EasClient,
    pts: List[PowerTransformer],
    transformer_to_suspect_end: Dict[str, Tuple[int, List[ConductingEquipment]]],
    name: str,
    description: str,
    tags: List[str],
    styles: List
) -> None:
    class_to_properties = {
        EnergyConsumer: {
            "name": lambda ec: ec.name,
            "type": lambda x: "ec"
        },
        PowerTransformer: {
            "consumer_count": _suspect_end_count_from(transformer_to_suspect_end),
            "type": lambda x: "pt"
        },
        AcLineSegment: {"name": lambda ec: ec.name},
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
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


async def _get_upstream_eq_up_to_transformer(ce: ConductingEquipment) -> Set[ConductingEquipment]:
    eqs = set()
    trace = normal_upstream_trace()
    phase_step = start_at(ce, PhaseCode.ABCN)
    trace.add_step_action(collect_eq_provider(eqs))
    trace.add_stop_condition(_is_transformer)

    await trace.run(start_item=phase_step, can_stop_on_start_item=False)
    return eqs


async def _is_transformer(ps: PhaseStep):
    return isinstance(ps.conducting_equipment, PowerTransformer)


def _suspect_end_count_from(pt_to_sus_end: Dict[str, Tuple[int, List[ConductingEquipment]]]):
    def fun(pt: PowerTransformer):
        count, suspect_eq = pt_to_sus_end.get(pt.mrid)
        return count if count else 0

    return fun


def to_geojson_feature_collection(psrs: List[PowerSystemResource], class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]]) -> FeatureCollection:
    features = []
    for psr in psrs:
        properties_map = class_to_properties.get(type(psr))

        if properties_map is not None:
            features.append(to_geojson_feature(psr, properties_map))

    return FeatureCollection(features)


def to_geojson_feature(psr: PowerSystemResource, property_map: Dict[str, Callable[[PowerSystemResource], Any]]) -> Union[Feature, None]:
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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
