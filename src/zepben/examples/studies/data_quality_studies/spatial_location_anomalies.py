#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple
import sys

from geojson import FeatureCollection
from zepben.eas import Mutation, GeoJsonOverlayInput, StudyResultInput, StudyInput
from zepben.ewb import (
    AcLineSegment,
    EnergyConsumer,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    NetworkTraceStep,
    PhaseCode,
    PowerTransformer,
    Tracing,
    connect_with_token,
    stop_at_open,
    upstream,
)
from zepben.ewb.services.network.tracing.networktrace.operators.network_state_operators import (
    NetworkStateOperators,
)

from dq_utils import (
    chunk,
    get_zone_mrids,
    line_length_m,
    load_config,
    no_anomaly_feature_collection,
    to_geojson_feature_collection,
    connect_rpc_from_config,
    create_eas_client_from_config,
)


LONG_SERVICE_DROP_M = 500.0
VERY_LONG_LINE_M = 2000.0


async def main():
    zone_mrids = get_zone_mrids(sys.argv, default=["CPM"])
    print(f"Start time: {datetime.now()}")

    config = load_config()
    rpc_channel = connect_rpc_from_config(config)
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()
    feeder_mrids = _collect_feeder_mrids(hierarchy.value.substations, zone_mrids)

    print(f"Collecting feeders from zones {', '.join(zone_mrids)}.")
    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")

    long_service_ecs: Set[EnergyConsumer] = set()
    ec_to_distance: Dict[str, float] = {}
    very_long_lines: Set[AcLineSegment] = set()

    for feeders in chunk(feeder_mrids, 3):
        rpc_channel = connect_rpc_from_config(config)
        for feeder_mrid in feeders:
            result = await _fetch_spatial_anomalies(feeder_mrid, rpc_channel)
            if result is None:
                continue
            feeder_ecs, feeder_distances, feeder_lines = result
            long_service_ecs.update(feeder_ecs)
            ec_to_distance.update(feeder_distances)
            very_long_lines.update(feeder_lines)

    style_path = Path(__file__).resolve().parent / "style_spatial_location.json"
    styles = json.load(open(style_path, "r"))
    result_specs = [
        (
            "Long service drops",
            _build_ec_distance_result(
                "Long service drops",
                list(long_service_ecs),
                ec_to_distance,
                style_ids=["dq-long-service-drop"],
            ),
        ),
        (
            "Very long line segments",
            _build_line_result(
                "Very long line segments",
                list(very_long_lines),
                style_ids=["dq-very-long-lines"],
                issue="very_long_line",
            ),
        ),
    ]
    results = []
    for name, result in result_specs:
        if result is not None:
            results.append(result)
        else:
            results.append(
                StudyResultInput(
                    name=f"No anomalies detected: {name}",
                    sections=[],
                    geo_json_overlay=GeoJsonOverlayInput(
                        data=no_anomaly_feature_collection(),
                        styles=["dq-no-anomalies"],
                    ),
                )
            )

    eas_client = create_eas_client_from_config(config)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=f"Spatial/location anomalies ({', '.join(zone_mrids)})",
            description="Long service drops and unusually long line segments.",
            tags=["dq_spatial_location", "-".join(zone_mrids)],
            results=results,
            styles=styles,
        )
        ]
    )
    )
    await eas_client.close()
    print("Uploaded Study")
    print(f"Finish time: {datetime.now()}")


def connect_rpc_from_config(config):
    return connect_rpc_from_config(config)


def _collect_feeder_mrids(substations: Dict, zone_mrids: List[str]) -> List[str]:
    feeder_mrids: List[str] = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)
    return feeder_mrids


async def _fetch_spatial_anomalies(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[Set[EnergyConsumer], Dict[str, float], Set[AcLineSegment]] | None:
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)
    result = await client.get_equipment_container(
        mrid=feeder_mrid,
        expected_class=Feeder,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return None

    network = client.service
    print(f"Finished fetching Feeder {feeder_mrid}")

    return await analyze_network(network)


async def analyze_network(
    network,
) -> Tuple[Set[EnergyConsumer], Dict[str, float], Set[AcLineSegment]]:
    await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)

    long_service_ecs: Set[EnergyConsumer] = set()
    ec_to_distance: Dict[str, float] = {}
    for ec in network.objects(EnergyConsumer):
        path_lines = await _get_upstream_lines_to_transformer(ec)
        distance = sum(line_length_m(line) for line in path_lines)
        ec_to_distance[ec.mrid] = distance
        if distance >= LONG_SERVICE_DROP_M:
            long_service_ecs.add(ec)

    very_long_lines = {line for line in network.objects(AcLineSegment) if line_length_m(line) >= VERY_LONG_LINE_M}

    return long_service_ecs, ec_to_distance, very_long_lines


async def _get_upstream_lines_to_transformer(ec: EnergyConsumer) -> Set[AcLineSegment]:
    lines = set()

    async def collect_lines(ps: NetworkTraceStep, _):
        line = ps.path.traversed_ac_line_segment
        if line is not None:
            lines.add(line)
        if isinstance(ps.path.to_equipment, AcLineSegment):
            lines.add(ps.path.to_equipment)
        if isinstance(ps.path.from_equipment, AcLineSegment):
            lines.add(ps.path.from_equipment)

    await (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_condition(stop_at_open())
        .add_step_action(collect_lines)
        .add_stop_condition(_is_transformer)
    ).run(start=ec, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return lines


def _is_transformer(ps: NetworkTraceStep, _context=None) -> bool:
    return isinstance(ps.path.to_equipment, PowerTransformer)


def _build_ec_distance_result(
    name: str,
    ecs: List[EnergyConsumer],
    ec_to_distance: Dict[str, float],
    style_ids: List[str],
) -> StudyResultInput | None:
    if not ecs:
        return None

    def distance(ec: EnergyConsumer) -> float:
        return float(ec_to_distance.get(ec.mrid, 0.0))

    def label(ec: EnergyConsumer) -> str:
        value = distance(ec)
        return f"{value:.0f} m" if value > 0 else "n/a"

    class_to_properties = {
        EnergyConsumer: {
            "distance_m": distance,
            "distance_label": label,
            "type": lambda _: "ec",
        }
    }
    feature_collection: FeatureCollection = to_geojson_feature_collection(ecs, class_to_properties)
    if not feature_collection.features:
        return None
    return StudyResultInput(
        name=name,
        sections=[],
        geo_json_overlay=GeoJsonOverlayInput(
            data=feature_collection,
            styles=style_ids,
        ),
    )


def _build_line_result(
    name: str,
    lines: List[AcLineSegment],
    style_ids: List[str],
    issue: str,
) -> StudyResultInput | None:
    if not lines:
        return None
    class_to_properties = {
        AcLineSegment: {
            "issue": lambda _: issue,
            "name": lambda line: line.name or line.mrid,
            "type": lambda _: "line",
        }
    }
    feature_collection: FeatureCollection = to_geojson_feature_collection(lines, class_to_properties)
    if not feature_collection.features:
        return None
    return StudyResultInput(
        name=name,
        sections=[],
        geo_json_overlay=GeoJsonOverlayInput(
            data=feature_collection,
            styles=style_ids,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
