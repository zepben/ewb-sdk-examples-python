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
from zepben.eas import EasClient, Mutation, GeoJsonOverlayInput, StudyResultInput, StudyInput
from zepben.ewb import (
    AcLineSegment,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    Switch,
    connect_with_token,
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

    loop_lines: Set[AcLineSegment] = set()
    switch_terminal_issues: Set[Switch] = set()

    for feeders in chunk(feeder_mrids, 3):
        rpc_channel = _connect_rpc(config)
        for feeder_mrid in feeders:
            result = await _fetch_protection_directionality_issues(feeder_mrid, rpc_channel)
            if result is None:
                continue
            loop_segments, switch_issues = result
            loop_lines.update(loop_segments)
            switch_terminal_issues.update(switch_issues)

    style_path = Path(__file__).resolve().parent / "style_protection_directionality.json"
    styles = json.load(open(style_path, "r"))
    result_specs = [
        (
            "Loops without switches",
            _build_line_result(
                "Loops without switches",
                list(loop_lines),
                style_ids=["dq-loop-no-switch"],
                issue="loop_without_switch",
            ),
        ),
        (
            "Switch terminal anomalies",
            _build_switch_result(
                "Switch terminal anomalies",
                list(switch_terminal_issues),
                style_ids=["dq-switch-terminal-issue"],
                issue="switch_terminal_issue",
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
            name=f"Protection and directionality anomalies ({', '.join(zone_mrids)})",
            description="Loops without switches and switch terminal count anomalies.",
            tags=["dq_protection_directionality", "-".join(zone_mrids)],
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


async def _fetch_protection_directionality_issues(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[Set[AcLineSegment], Set[Switch]] | None:
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
) -> Tuple[Set[AcLineSegment], Set[Switch]]:
    loop_lines = _find_loops_without_switch(network)

    switch_terminal_issues = set()
    for sw in network.objects(Switch):
        terminals = [t for t in sw.terminals if t.connectivity_node is not None]
        if len(terminals) != 2:
            switch_terminal_issues.add(sw)

    return loop_lines, switch_terminal_issues


def _find_loops_without_switch(network) -> Set[AcLineSegment]:
    lines = list(network.objects(AcLineSegment))
    if not lines:
        return set()

    parent = {}

    def find(node):
        if node not in parent:
            parent[node] = node
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(a, b):
        ra = find(a)
        rb = find(b)
        if ra != rb:
            parent[rb] = ra

    line_nodes: Dict[AcLineSegment, Tuple[object, object]] = {}
    for line in lines:
        nodes = [t.connectivity_node for t in line.terminals if t.connectivity_node is not None]
        if len(nodes) < 2:
            continue
        n1, n2 = nodes[0], nodes[1]
        line_nodes[line] = (n1, n2)
        union(n1, n2)

    component_edges: Dict[object, int] = {}
    component_nodes: Dict[object, Set[object]] = {}
    component_lines: Dict[object, Set[AcLineSegment]] = {}
    for line, (n1, n2) in line_nodes.items():
        root = find(n1)
        component_edges[root] = component_edges.get(root, 0) + 1
        component_nodes.setdefault(root, set()).update([n1, n2])
        component_lines.setdefault(root, set()).add(line)

    component_has_switch: Dict[object, bool] = {root: False for root in component_nodes}
    for sw in network.objects(Switch):
        for terminal in sw.terminals:
            node = terminal.connectivity_node
            if node is None:
                continue
            root = find(node)
            if root in component_has_switch:
                component_has_switch[root] = True

    loop_lines: Set[AcLineSegment] = set()
    for root, edges in component_edges.items():
        nodes = component_nodes.get(root, set())
        if not nodes:
            continue
        if edges >= len(nodes) and not component_has_switch.get(root, False):
            loop_lines.update(component_lines.get(root, set()))

    return loop_lines


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


def _build_switch_result(
    name: str,
    switches: List[Switch],
    style_ids: List[str],
    issue: str,
) -> StudyResultInput | None:
    if not switches:
        return None
    class_to_properties = {
        Switch: {
            "issue": lambda _: issue,
            "name": lambda sw: sw.name or sw.mrid,
            "type": lambda _: "switch",
            "terminal_count": lambda sw: len([t for t in sw.terminals if t.connectivity_node is not None]),
        }
    }
    feature_collection: FeatureCollection = to_geojson_feature_collection(switches, class_to_properties)
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
