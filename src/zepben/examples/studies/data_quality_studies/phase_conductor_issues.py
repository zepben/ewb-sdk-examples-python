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
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import GeoJsonOverlay, Result, Study
from zepben.ewb import (
    AcLineSegment,
    ConductingEquipment,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    connect_with_token,
)

from dq_utils import (
    chunk,
    get_zone_mrids,
    load_config,
    no_anomaly_feature_collection,
    point_feature,
    terminal_phase_code,
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

    phase_mismatch_features = []
    missing_phase_lines: Set[AcLineSegment] = set()

    for feeders in chunk(feeder_mrids, 3):
        rpc_channel = _connect_rpc(config)
        for feeder_mrid in feeders:
            result = await _fetch_phase_issues(feeder_mrid, rpc_channel)
            if result is None:
                continue
            mismatch_features, missing_lines = result
            phase_mismatch_features.extend(mismatch_features)
            missing_phase_lines.update(missing_lines)

    style_path = Path(__file__).resolve().parent / "style_phase_conductor.json"
    styles = json.load(open(style_path, "r"))
    result_specs = [
        (
            "Phase mismatch at nodes",
            _build_feature_result(
                "Phase mismatch at nodes",
                phase_mismatch_features,
                style_ids=["dq-phase-mismatch-node"],
            ),
        ),
        (
            "Lines missing phase information",
            _build_line_result(
                "Lines missing phase information",
                list(missing_phase_lines),
                style_ids=["dq-missing-phase-lines"],
            ),
        ),
    ]
    results = []
    for name, result in result_specs:
        if result is not None:
            results.append(result)
        else:
            results.append(
                Result(
                    name=f"No anomalies detected: {name}",
                    geo_json_overlay=GeoJsonOverlay(
                        data=no_anomaly_feature_collection(),
                        styles=["dq-no-anomalies"],
                    ),
                )
            )

    eas_client = EasClient(host=config["host"], port=config["rpc_port"], protocol="https", access_token=config["access_token"])
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await eas_client.async_upload_study(
        Study(
            name=f"Phase and conductor issues ({', '.join(zone_mrids)})",
            description="Connectivity nodes with mixed phases and lines missing phase info.",
            tags=["dq_phase_conductor", "-".join(zone_mrids)],
            results=results,
            styles=styles,
        )
    )
    await eas_client.aclose()
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


async def _fetch_phase_issues(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[List, Set[AcLineSegment]] | None:
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
) -> Tuple[List, Set[AcLineSegment]]:
    node_to_terminals: Dict[object, List[object]] = {}
    node_to_equipment: Dict[object, ConductingEquipment] = {}
    for ce in network.objects(ConductingEquipment):
        for terminal in ce.terminals:
            node = terminal.connectivity_node
            if node is None:
                continue
            node_to_terminals.setdefault(node, []).append(terminal)
            node_to_equipment.setdefault(node, ce)

    phase_mismatch_features = []
    for node, terminals in node_to_terminals.items():
        phase_codes = []
        for terminal in terminals:
            phase_code = terminal_phase_code(terminal)
            if phase_code is not None:
                phase_codes.append(str(phase_code))
        unique_phases = sorted(set(phase_codes))
        if len(unique_phases) > 1:
            equipment = node_to_equipment.get(node)
            if equipment is None or equipment.location is None:
                continue
            points = list(equipment.location.points)
            if not points:
                continue
            pt = points[0]
            feature = point_feature(
                f"phase-mismatch-{getattr(node, 'mrid', id(node))}",
                pt.x_position,
                pt.y_position,
                {
                    "issue": "phase_mismatch",
                    "phases": ", ".join(unique_phases),
                    "type": "node",
                },
            )
            phase_mismatch_features.append(feature)

    missing_phase_lines: Set[AcLineSegment] = set()
    for line in network.objects(AcLineSegment):
        terminal_phases = []
        for terminal in line.terminals:
            phase_code = terminal_phase_code(terminal)
            if phase_code is not None:
                terminal_phases.append(phase_code)
        if not terminal_phases:
            missing_phase_lines.add(line)

    return phase_mismatch_features, missing_phase_lines


def _build_feature_result(
    name: str,
    features: List,
    style_ids: List[str],
) -> Result | None:
    if not features:
        return None
    feature_collection = FeatureCollection(features)
    if not feature_collection.features:
        return None
    return Result(
        name=name,
        geo_json_overlay=GeoJsonOverlay(
            data=feature_collection,
            styles=style_ids,
        ),
    )


def _build_line_result(
    name: str,
    lines: List[AcLineSegment],
    style_ids: List[str],
) -> Result | None:
    if not lines:
        return None
    class_to_properties = {
        AcLineSegment: {
            "issue": lambda _: "missing_phase",
            "name": lambda line: line.name or line.mrid,
            "type": lambda _: "line",
        }
    }
    feature_collection: FeatureCollection = to_geojson_feature_collection(lines, class_to_properties)
    if not feature_collection.features:
        return None
    return Result(
        name=name,
        geo_json_overlay=GeoJsonOverlay(
            data=feature_collection,
            styles=style_ids,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
