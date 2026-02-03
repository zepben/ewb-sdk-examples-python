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
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import Study, Result, GeoJsonOverlay
from zepben.ewb import (
    PowerTransformer,
    ConductingEquipment,
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
    pv_only = True  # Set False to include all PowerElectronicsConnections, not just those with PV units.
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
    transformer_to_stats: Dict[str, Tuple[int, int, int, int]] = {}

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
            futures.append(
                asyncio.ensure_future(
                    fetch_feeder_and_capacity_stats(
                        feeder_mrid,
                        rpc_channel,
                        pv_only=pv_only,
                    )
                )
            )

        for future in futures:
            transformers, stats = await future
            if transformers:  # Empty if the feeder failed
                all_transformers.extend(transformers)
                transformer_to_stats.update(stats)

    print(f"Created Study for {len(all_transformers)} transformers")

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await upload_capacity_percent_study(
        eas_client,
        all_transformers,
        transformer_to_stats,
        name=f"PEC Capacity % vs Transformer Rating ({', '.join(zone_mrids)})",
        description="Compares sum of PowerElectronicsConnection capacity (PV only by default) to transformer rating.",
        tags=["pec_capacity_percent", "-".join(zone_mrids)],
        styles=json.load(open("style_pec_capacity_percent.json", "r")),
    )
    await eas_client.aclose()
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


async def fetch_feeder_and_capacity_stats(
    feeder_mrid: str,
    rpc_channel,
    pv_only: bool,
) -> Tuple[List[PowerTransformer], Dict[str, Tuple[int, int, int, int]]]:
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

    transformers: List[PowerTransformer] = []
    transformer_to_stats: Dict[str, Tuple[int, int, int, int]] = {}

    print(f"Tracing downstream transformers for feeder {feeder_mrid}")
    for io in (pt for pt in network.objects(PowerTransformer)):
        pt: PowerTransformer = io
        transformers.append(pt)
        downstream_equipment = await get_downstream_eq(pt)

        pecs = [
            eq for eq in downstream_equipment
            if isinstance(eq, PowerElectronicsConnection)
        ]
        if pv_only:
            pecs = [pec for pec in pecs if _pec_has_pv_unit(pec)]

        capacity_va = sum(_pec_capacity_va(pec) for pec in pecs)
        rating_va = _transformer_rating_va(pt)
        percent = round((capacity_va / rating_va) * 100) if rating_va else 0

        transformer_to_stats[pt.mrid] = (capacity_va, rating_va, percent, len(pecs))

    return transformers, transformer_to_stats


def _pec_has_pv_unit(pec: PowerElectronicsConnection) -> bool:
    return any(isinstance(unit, PhotoVoltaicUnit) for unit in pec.units)


def _pec_capacity_va(pec: PowerElectronicsConnection) -> int:
    if pec.rated_s is not None:
        return int(pec.rated_s)
    unit_capacity = sum(unit.max_p for unit in pec.units if unit.max_p is not None)
    return int(unit_capacity) if unit_capacity else 0


def _transformer_rating_va(pt: PowerTransformer) -> int:
    ratings: List[int] = []
    for end in pt.ends:
        if end.rated_s is not None:
            ratings.append(end.rated_s)
        else:
            for rating in end.s_ratings:
                if rating and rating.rated_s is not None:
                    ratings.append(rating.rated_s)
    return max(ratings) if ratings else 0


async def upload_capacity_percent_study(
    eas_client: EasClient,
    transformers: List[PowerTransformer],
    transformer_to_stats: Dict[str, Tuple[int, int, int, int]],
    name: str,
    description: str,
    tags: List[str],
    styles: List,
) -> None:

    class_to_properties = {
        PowerTransformer: {
            "capacity_percent": _capacity_percent_from(transformer_to_stats),
            "capacity_percent_label": _capacity_percent_label_from(transformer_to_stats),
            "pec_capacity_va": _pec_capacity_from(transformer_to_stats),
            "transformer_rating_va": _transformer_rating_from(transformer_to_stats),
            "pec_count": _pec_count_from(transformer_to_stats),
            "type": lambda x: "pt",
        }
    }
    feature_collection = to_geojson_feature_collection(transformers, class_to_properties)
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
                        styles=[s["id"] for s in styles],
                    ),
                )
            ],
            styles=styles,
        )
    )
    print(f"Study response: {response}")


def _capacity_percent_from(pt_to_stats: Dict[str, Tuple[int, int, int, int]]):
    def fun(pt: PowerTransformer):
        capacity, rating, percent, pec_count = pt_to_stats.get(pt.mrid, (0, 0, 0, 0))
        return percent

    return fun


def _capacity_percent_label_from(pt_to_stats: Dict[str, Tuple[int, int, int, int]]):
    def fun(pt: PowerTransformer):
        capacity, rating, percent, pec_count = pt_to_stats.get(pt.mrid, (0, 0, 0, 0))
        return "n/a" if rating == 0 else f"{percent}%"

    return fun


def _pec_capacity_from(pt_to_stats: Dict[str, Tuple[int, int, int, int]]):
    def fun(pt: PowerTransformer):
        capacity, rating, percent, pec_count = pt_to_stats.get(pt.mrid, (0, 0, 0, 0))
        return capacity

    return fun


def _transformer_rating_from(pt_to_stats: Dict[str, Tuple[int, int, int, int]]):
    def fun(pt: PowerTransformer):
        capacity, rating, percent, pec_count = pt_to_stats.get(pt.mrid, (0, 0, 0, 0))
        return rating

    return fun


def _pec_count_from(pt_to_stats: Dict[str, Tuple[int, int, int, int]]):
    def fun(pt: PowerTransformer):
        capacity, rating, percent, pec_count = pt_to_stats.get(pt.mrid, (0, 0, 0, 0))
        return pec_count

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
