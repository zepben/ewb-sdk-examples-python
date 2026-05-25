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
    PowerTransformer,
    ConductingEquipment,
    EnergyConsumer,
    PhotoVoltaicUnit,
    PowerElectronicsConnection,
    NetworkConsumerClient,
    PhaseCode,
    Feeder,
    PowerSystemResource,
    Location,
    connect_with_token,
    NetworkTraceStep,
    Tracing,
    downstream,
    IncludedEnergizedContainers,
)


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

    all_transformers: List[PowerTransformer] = []
    transformer_to_stats: Dict[str, Tuple[int, int, int]] = {}

    # Process the feeders in batches of 3, using asyncio, for performance
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
            futures.append(asyncio.ensure_future(fetch_feeder_and_pv_stats(feeder_mrid, rpc_channel)))

        for future in futures:
            transformers, stats = await future
            if transformers:  # Empty if the feeder failed
                all_transformers.extend(transformers)
                transformer_to_stats.update(stats)

    print(f"Created Study for {len(all_transformers)} transformers")

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"], asynchronous=True, enable_legacy_methods=True)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await upload_pv_percent_study(
        eas_client,
        all_transformers,
        transformer_to_stats,
        name=f"PV % by Transformer ({', '.join(zone_mrids)})",
        description="Percentage of EnergyConsumers with PV (PhotoVoltaicUnit) downstream of each transformer.",
        tags=["pv_percent_by_transformer", "-".join(zone_mrids)],
        styles=json.load(open("style_pv_percent.json", "r")),
    )
    await eas_client.close()
    print("Uploaded Study")

    print(f"Finish time: {datetime.now()}")


def collect_eq_provider(collection: Set[ConductingEquipment]):

    async def collect_equipment(ps: NetworkTraceStep, _):
        collection.add(ps.path.to_equipment)

    return collect_equipment


async def get_downstream_eq(ce: ConductingEquipment) -> Set[ConductingEquipment]:
    equipment_set = set()

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(collect_eq_provider(equipment_set))
    ).run(start=ce, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return equipment_set


async def fetch_feeder_and_pv_stats(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[List[PowerTransformer], Dict[str, Tuple[int, int, int]]]:
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
        return [], {}

    network = client.service
    print(f"Finished fetching Feeder {feeder_mrid}")

    pv_consumers = _find_pv_energy_consumers(network)

    transformers: List[PowerTransformer] = []
    transformer_to_stats: Dict[str, Tuple[int, int, int]] = {}

    print(f"Tracing downstream transformers for feeder {feeder_mrid}")
    for io in (pt for pt in network.objects(PowerTransformer)):
        pt: PowerTransformer = io
        transformers.append(pt)
        downstream_equipment = await get_downstream_eq(pt)
        downstream_ecs = [eq for eq in downstream_equipment if isinstance(eq, EnergyConsumer)]
        total_ec = len(downstream_ecs)
        pv_ec = sum(1 for ec in downstream_ecs if ec.mrid in pv_consumers)
        pv_percent = round((pv_ec / total_ec) * 100) if total_ec else 0
        transformer_to_stats[pt.mrid] = (pv_ec, total_ec, pv_percent)

    return transformers, transformer_to_stats


def _find_pv_energy_consumers(network) -> Set[str]:
    pv_usage_points: Set[str] = set()
    pv_nodes: Set[str] = set()

    # PV units can be directly linked to usage points or via their power electronics connection
    for pv in network.objects(PhotoVoltaicUnit):
        for up in pv.usage_points:
            pv_usage_points.add(up.mrid)
        if pv.power_electronics_connection:
            for up in pv.power_electronics_connection.usage_points:
                pv_usage_points.add(up.mrid)

    # PV units also appear on power electronics connections
    for pec in network.objects(PowerElectronicsConnection):
        if any(isinstance(unit, PhotoVoltaicUnit) for unit in pec.units):
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


async def upload_pv_percent_study(
    eas_client: EasClient,
    transformers: List[PowerTransformer],
    transformer_to_stats: Dict[str, Tuple[int, int, int]],
    name: str,
    description: str,
    tags: List[str],
    styles: List,
) -> None:

    class_to_properties = {
        PowerTransformer: {
            "pv_percent": _pv_percent_from(transformer_to_stats),
            "pv_percent_label": _pv_percent_label_from(transformer_to_stats),
            "pv_ec_count": _pv_count_from(transformer_to_stats),
            "ec_count": _ec_count_from(transformer_to_stats),
            "type": lambda x: "pt",
        }
    }
    feature_collection = to_geojson_feature_collection(transformers, class_to_properties)
    response = await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=name,
            description=description,
            tags=tags,
            results=[
                StudyResultInput(
                    name=name,
                    sections=[],
                    geo_json_overlay=GeoJsonOverlayInput(
                        data=feature_collection,
                        styles=[s["id"] for s in styles],
                    ),
                )
            ],
            styles=styles,
        )
        ]
    )
    )
    print(f"Study response: {response}")


def _pv_percent_from(pt_to_stats: Dict[str, Tuple[int, int, int]]):
    def fun(pt: PowerTransformer):
        pv_count, total, percent = pt_to_stats.get(pt.mrid, (0, 0, 0))
        return percent

    return fun


def _pv_percent_label_from(pt_to_stats: Dict[str, Tuple[int, int, int]]):
    def fun(pt: PowerTransformer):
        pv_count, total, percent = pt_to_stats.get(pt.mrid, (0, 0, 0))
        return f"{percent}%"

    return fun


def _pv_count_from(pt_to_stats: Dict[str, Tuple[int, int, int]]):
    def fun(pt: PowerTransformer):
        pv_count, total, percent = pt_to_stats.get(pt.mrid, (0, 0, 0))
        return pv_count

    return fun


def _ec_count_from(pt_to_stats: Dict[str, Tuple[int, int, int]]):
    def fun(pt: PowerTransformer):
        pv_count, total, percent = pt_to_stats.get(pt.mrid, (0, 0, 0))
        return total

    return fun


def to_geojson_feature_collection(
    psrs: List[PowerSystemResource],
    class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]],
) -> FeatureCollection:

    features = []
    for psr in psrs:
        properties_map = class_to_properties.get(type(psr))
        if properties_map is None:
            continue
        feature = to_geojson_feature(psr, properties_map)
        if feature is not None:
            features.append(feature)

    return FeatureCollection(features)


def to_geojson_feature(
    psr: PowerSystemResource,
    property_map: Dict[str, Callable[[PowerSystemResource], Any]],
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
