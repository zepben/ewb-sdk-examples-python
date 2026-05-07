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
    AcLineSegment,
    EnergyConsumer,
    PhotoVoltaicUnit,
    PowerElectronicsConnection,
    PowerTransformer,
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
    transformer_to_metrics: Dict[str, Dict[str, float]] = {}

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
            futures.append(asyncio.ensure_future(fetch_transformer_densities(feeder_mrid, rpc_channel)))

        for future in futures:
            transformers, metrics = await future
            if transformers:
                all_transformers.extend(transformers)
                transformer_to_metrics.update(metrics)

    print(f"Creating study for {len(all_transformers)} transformers")

    styles = json.load(open("style_transformer_density.json", "r"))
    results = [
        _build_density_result(
            "EnergyConsumer density (/100m)",
            all_transformers,
            transformer_to_metrics,
            metric_key="ec_density",
            label_key="ec_density_label",
            style_ids=["ec-density-circle", "ec-density-label"],
        ),
        _build_density_result(
            "UsagePoint density (/100m)",
            all_transformers,
            transformer_to_metrics,
            metric_key="up_density",
            label_key="up_density_label",
            style_ids=["up-density-circle", "up-density-label"],
        ),
        _build_density_result(
            "PV density (/100m)",
            all_transformers,
            transformer_to_metrics,
            metric_key="pv_density",
            label_key="pv_density_label",
            style_ids=["pv-density-circle", "pv-density-label"],
        ),
    ]

    results = [r for r in results if r is not None]
    if not results:
        print("No transformer features to display (missing locations). Study upload skipped.")
        return

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await eas_client.async_upload_study(
        Study(
            name=f"Transformer densities ({', '.join(zone_mrids)})",
            description="Downstream EC, UsagePoint and PV density per 100m of AC line segment.",
            tags=["transformer_density", "-".join(zone_mrids)],
            results=results,
            styles=styles,
        )
    )
    await eas_client.aclose()
    print("Uploaded Study")
    print(f"Finish time: {datetime.now()}")


async def fetch_transformer_densities(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[List[PowerTransformer], Dict[str, Dict[str, float]]]:
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
    transformer_to_metrics: Dict[str, Dict[str, float]] = {}
    for io in (pt for pt in network.objects(PowerTransformer)):
        pt: PowerTransformer = io
        transformers.append(pt)

        downstream_eq = await get_downstream_eq(pt)
        downstream_lines = {eq for eq in downstream_eq if isinstance(eq, AcLineSegment)}
        downstream_ecs = {eq for eq in downstream_eq if isinstance(eq, EnergyConsumer)}

        total_length_m = sum(_line_length_m(line) for line in downstream_lines)
        ec_count = len(downstream_ecs)
        up_count = _usage_point_count(downstream_ecs)
        pv_count = sum(1 for ec in downstream_ecs if ec.mrid in pv_consumers)

        ec_density = _safe_density_per_100m(ec_count, total_length_m)
        up_density = _safe_density_per_100m(up_count, total_length_m)
        pv_density = _safe_density_per_100m(pv_count, total_length_m)

        transformer_to_metrics[pt.mrid] = {
            "ec_density": ec_density,
            "ec_density_label": _density_label(ec_density),
            "up_density": up_density,
            "up_density_label": _density_label(up_density),
            "pv_density": pv_density,
            "pv_density_label": _density_label(pv_density),
        }

    return transformers, transformer_to_metrics


def _usage_point_count(ecs: Set[EnergyConsumer]) -> int:
    usage_points = set()
    for ec in ecs:
        for up in ec.usage_points:
            usage_points.add(up.mrid)
    return len(usage_points)


def _safe_density_per_100m(count: int, length_m: float) -> float:
    if length_m <= 0:
        return 0.0
    return (count / length_m) * 100.0


def _density_label(value: float) -> str:
    return f"{value:.2f}/100m" if value > 0 else "n/a"


def _line_length_m(line: AcLineSegment) -> float:
    return float(line.length or 0.0)


def collect_downstream_edges_provider(
    adjacency: Dict[PowerSystemResource, Set[PowerSystemResource]],
    nodes: Set[PowerSystemResource],
):

    async def collect_edges(ps: NetworkTraceStep, _):
        nodes.add(ps.path.from_equipment)
        nodes.add(ps.path.to_equipment)
        if ps.path.traced_externally:
            adjacency.setdefault(ps.path.from_equipment, set()).add(ps.path.to_equipment)

    return collect_edges


async def get_downstream_eq(ce: PowerTransformer) -> Set[PowerSystemResource]:
    nodes: Set[PowerSystemResource] = {ce}
    adjacency: Dict[PowerSystemResource, Set[PowerSystemResource]] = {}

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(collect_downstream_edges_provider(adjacency, nodes))
    ).run(start=ce, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return nodes


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


def _build_density_result(
    result_name: str,
    transformers: List[PowerTransformer],
    transformer_to_metrics: Dict[str, Dict[str, float]],
    metric_key: str,
    label_key: str,
    style_ids: List[str],
) -> Union[Result, None]:
    class_to_properties = {
        PowerTransformer: {
            metric_key: _metric_from(transformer_to_metrics, metric_key),
            label_key: _metric_from(transformer_to_metrics, label_key),
            "type": lambda x: "pt",
        },
    }
    feature_collection = to_geojson_feature_collection(transformers, class_to_properties)
    if not feature_collection.features:
        return None
    return Result(
        name=result_name,
        geo_json_overlay=GeoJsonOverlay(
            data=feature_collection,
            styles=style_ids,
        )
    )


def _metric_from(pt_to_metrics: Dict[str, Dict[str, float]], key: str):
    def fun(pt: PowerTransformer):
        info = pt_to_metrics.get(pt.mrid, {})
        return info.get(key, 0.0)

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
