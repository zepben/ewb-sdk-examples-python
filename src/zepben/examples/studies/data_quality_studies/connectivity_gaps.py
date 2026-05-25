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
from zepben.eas import EasClient, GeoJsonOverlayInput, StudyResultInput, StudyInput, Mutation
from zepben.ewb import (
    AcLineSegment,
    ConductingEquipment,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    NetworkTraceStep,
    PhaseCode,
    Tracing,
    connect_with_token,
    stop_at_open,
)

from dq_utils import (
    chunk,
    get_zone_mrids,
    load_config,
    no_anomaly_feature_collection,
    to_geojson_feature_collection,
)


async def main():
    zone_mrids = get_zone_mrids(sys.argv, default=["CPM"])
    print(f"Start time: {datetime.now()}")

    config = load_config()
    rpc_channel = _connect_rpc(config)
    client = NetworkConsumerClient(rpc_channel)
    hierarchy = (await client.get_network_hierarchy()).throw_on_error()

    feeder_mrids = _collect_feeder_mrids(hierarchy.value.substations, zone_mrids)
    print(f"Collecting feeders from zones {', '.join(zone_mrids)}.")
    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")

    open_ended_lines: Set[AcLineSegment] = set()
    disconnected_lines: Set[AcLineSegment] = set()

    for feeders in chunk(feeder_mrids, 3):
        rpc_channel = _connect_rpc(config)
        for feeder_mrid in feeders:
            result = await _fetch_connectivity_gaps(feeder_mrid, rpc_channel)
            if result is None:
                continue
            feeder_open_ends, feeder_disconnected = result
            open_ended_lines.update(feeder_open_ends)
            disconnected_lines.update(feeder_disconnected)

    style_path = Path(__file__).resolve().parent / "style_connectivity_gaps.json"
    styles = json.load(open(style_path, "r"))
    result_specs = [
        (
            "Open-ended line segments",
            _build_line_result(
                "Open-ended line segments",
                list(open_ended_lines),
                style_ids=["dq-open-ended-lines"],
                issue="open_end",
            ),
        ),
        (
            "Disconnected island lines",
            _build_line_result(
                "Disconnected island lines",
                list(disconnected_lines),
                style_ids=["dq-disconnected-lines"],
                issue="disconnected_island",
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

    eas_client = EasClient(host=config["host"], port=config["rpc_port"], protocol="https", access_token=config["access_token"], asynchronous=True, enable_legacy_methods=True)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=f"Connectivity gaps ({', '.join(zone_mrids)})",
            description="Highlights open-ended lines and disconnected line islands.",
            tags=["dq_connectivity_gaps", "-".join(zone_mrids)],
            results=results,
            styles=styles,
        )
    ]
    )
    )
    await eas_client.close()
    print("Uploaded Study")
    print(f"Finish time: {datetime.now()}")


def _connect_rpc(config):
    return connect_with_token(
        host=config["host"],
        access_token=config["access_token"],
        rpc_port=config["rpc_port"],
        ca_filename=config.get("ca_filename"),
        timeout_seconds=config.get("timeout_seconds", 5),
        debug=bool(config.get("debug", False)),
        skip_connection_test=bool(config.get("skip_connection_test", False)),
    )


def _collect_feeder_mrids(substations: Dict, zone_mrids: List[str]) -> List[str]:
    feeder_mrids: List[str] = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)
    return feeder_mrids


async def _fetch_connectivity_gaps(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[Set[AcLineSegment], Set[AcLineSegment]] | None:
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

    return await analyze_network(network, feeder_mrid)


async def analyze_network(
    network,
    feeder_mrid: str,
) -> Tuple[Set[AcLineSegment], Set[AcLineSegment]]:
    node_to_equipment: Dict[object, Set[ConductingEquipment]] = {}
    for ce in network.objects(ConductingEquipment):
        for terminal in ce.terminals:
            node = terminal.connectivity_node
            if node is None:
                continue
            node_to_equipment.setdefault(node, set()).add(ce)

    open_ended_lines = set()
    for line in network.objects(AcLineSegment):
        for terminal in line.terminals:
            node = terminal.connectivity_node
            if node and len(node_to_equipment.get(node, set())) == 1:
                open_ended_lines.add(line)
                break

    disconnected_lines = await _disconnected_island_lines(network, feeder_mrid)
    return open_ended_lines, disconnected_lines


async def _disconnected_island_lines(network, feeder_mrid: str) -> Set[AcLineSegment]:
    feeder_head = None
    for feeder in network.objects(Feeder):
        if feeder.mrid == feeder_mrid and feeder.normal_head_terminal:
            feeder_head = feeder.normal_head_terminal.conducting_equipment
            break
    if feeder_head is None:
        print(f"Feeder {feeder_mrid} has no normal head terminal; skipping disconnected island check.")
        return set()

    reachable: Set[ConductingEquipment] = set()

    async def collect(ps: NetworkTraceStep, _):
        if ps.path.from_equipment is not None:
            reachable.add(ps.path.from_equipment)
        if ps.path.to_equipment is not None:
            reachable.add(ps.path.to_equipment)

    await (
        Tracing.network_trace()
        .add_condition(stop_at_open())
        .add_step_action(collect)
    ).run(start=feeder_head, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    feeder_lines = [
        line for line in network.objects(AcLineSegment) if _belongs_to_feeder(line, feeder_mrid)
    ]
    reachable_lines = {eq for eq in reachable if isinstance(eq, AcLineSegment)}
    return set(feeder_lines) - reachable_lines


def _belongs_to_feeder(psr, feeder_mrid: str) -> bool:
    feeders = getattr(psr, "normal_feeders", []) or []
    return any(feeder.mrid == feeder_mrid for feeder in feeders)


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
