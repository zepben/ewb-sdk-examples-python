#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

from zepben.eas import Mutation, StudyInput

from zepben.ewb import (
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    connect_with_token,
)

import asset_attribute_inconsistencies as aa
import connectivity_gaps as cg
import consumer_mapping_issues as cm
import phase_conductor_issues as pc
import protection_directionality_anomalies as pd
import spatial_location_anomalies as sl
from dq_utils import (
    chunk,
    connect_rpc_from_config,
    create_eas_client_from_config,
    get_zone_mrids,
    load_config,
)

BATCH_SIZE = 4


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

    open_ended_lines: Set = set()
    disconnected_lines: Set = set()
    unserved_ecs: Set = set()
    missing_lv_feeder_ecs: Set = set()
    no_load_transformers: Set = set()
    phase_mismatch_features: List = []
    missing_phase_lines: Set = set()
    zero_length_lines: Set = set()
    missing_impedance_lines: Set = set()
    missing_rating_transformers: Set = set()
    missing_impedance_transformers: Set = set()
    missing_normal_state_switches: Set = set()
    loop_lines: Set = set()
    switch_terminal_issues: Set = set()
    long_service_ecs: Set = set()
    ec_to_distance: Dict[str, float] = {}
    very_long_lines: Set = set()

    for feeders in chunk(feeder_mrids, BATCH_SIZE):
        rpc_channel = connect_rpc_from_config(config)
        tasks = [
            asyncio.create_task(_process_feeder(feeder_mrid, rpc_channel))
            for feeder_mrid in feeders
        ]
        for result in await asyncio.gather(*tasks):
            if result is None:
                continue
            open_ended_lines.update(result["open_ended_lines"])
            disconnected_lines.update(result["disconnected_lines"])
            unserved_ecs.update(result["unserved_ecs"])
            missing_lv_feeder_ecs.update(result["missing_lv_feeder_ecs"])
            no_load_transformers.update(result["no_load_transformers"])
            phase_mismatch_features.extend(result["phase_mismatch_features"])
            missing_phase_lines.update(result["missing_phase_lines"])
            zero_length_lines.update(result["zero_length_lines"])
            missing_impedance_lines.update(result["missing_impedance_lines"])
            missing_rating_transformers.update(result["missing_rating_transformers"])
            missing_impedance_transformers.update(result["missing_impedance_transformers"])
            missing_normal_state_switches.update(result["missing_normal_state_switches"])
            loop_lines.update(result["loop_lines"])
            switch_terminal_issues.update(result["switch_terminal_issues"])
            long_service_ecs.update(result["long_service_ecs"])
            ec_to_distance.update(result["ec_to_distance"])
            very_long_lines.update(result["very_long_lines"])

    results, detected_tests, used_style_ids = _build_results(
        open_ended_lines,
        disconnected_lines,
        unserved_ecs,
        missing_lv_feeder_ecs,
        no_load_transformers,
        phase_mismatch_features,
        missing_phase_lines,
        zero_length_lines,
        missing_impedance_lines,
        missing_rating_transformers,
        missing_impedance_transformers,
        missing_normal_state_switches,
        loop_lines,
        switch_terminal_issues,
        long_service_ecs,
        ec_to_distance,
        very_long_lines,
    )

    if not results:
        print("No anomalies detected across all tests. Study upload skipped.")
        return

    styles = _load_styles(used_style_ids)
    description = _description_from_tests(detected_tests)
    tags = [_slugify(test) for test in detected_tests]

    eas_client = create_eas_client_from_config(config)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=f"Data quality summary ({', '.join(zone_mrids)})",
            description=description,
            tags=tags,
            results=results,
            styles=styles,
        )
    ]
    )
    )
    await eas_client.close()
    print("Uploaded Study")
    print(f"Finish time: {datetime.now()}")


def _collect_feeder_mrids(substations: Dict, zone_mrids: List[str]) -> List[str]:
    feeder_mrids: List[str] = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)
    return feeder_mrids


async def _fetch_feeder_network(feeder_mrid: str, rpc_channel):
    print(f"Fetching Feeder {feeder_mrid}")
    start = time.perf_counter()
    client = NetworkConsumerClient(rpc_channel)
    result = await client.get_equipment_container(
        mrid=feeder_mrid,
        expected_class=Feeder,
        include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return None
    elapsed = time.perf_counter() - start
    print(f"Finished fetching Feeder {feeder_mrid} ({elapsed:.2f}s)")
    return client.service


async def _process_feeder(feeder_mrid: str, rpc_channel):
    feeder_start = time.perf_counter()
    network = await _fetch_feeder_network(feeder_mrid, rpc_channel)
    if network is None:
        return None

    async def timed(label: str, coro):
        start = time.perf_counter()
        result = await coro
        elapsed = time.perf_counter() - start
        print(f"[{feeder_mrid}] {label}: {elapsed:.2f}s")
        return result

    feeder_open, feeder_disconnected = await timed(
        "connectivity_gaps",
        cg.analyze_network(network, feeder_mrid),
    )
    feeder_unserved, feeder_missing_lv, feeder_no_load = await timed(
        "consumer_mapping",
        cm.analyze_network(network, feeder_mrid),
    )
    mismatch_features, missing_lines = await timed(
        "phase_conductor",
        pc.analyze_network(network),
    )
    z_lines, i_lines, t_missing, t_imp_missing, s_missing = await timed(
        "asset_attributes",
        aa.analyze_network(network),
    )
    loop_segments, switch_issues = await timed(
        "protection_directionality",
        pd.analyze_network(network),
    )
    feeder_ecs, feeder_distances, feeder_lines = await timed(
        "spatial_location",
        sl.analyze_network(network),
    )

    total = time.perf_counter() - feeder_start
    print(f"[{feeder_mrid}] total: {total:.2f}s")

    return {
        "open_ended_lines": feeder_open,
        "disconnected_lines": feeder_disconnected,
        "unserved_ecs": feeder_unserved,
        "missing_lv_feeder_ecs": feeder_missing_lv,
        "no_load_transformers": feeder_no_load,
        "phase_mismatch_features": mismatch_features,
        "missing_phase_lines": missing_lines,
        "zero_length_lines": z_lines,
        "missing_impedance_lines": i_lines,
        "missing_rating_transformers": t_missing,
        "missing_impedance_transformers": t_imp_missing,
        "missing_normal_state_switches": s_missing,
        "loop_lines": loop_segments,
        "switch_terminal_issues": switch_issues,
        "long_service_ecs": feeder_ecs,
        "ec_to_distance": feeder_distances,
        "very_long_lines": feeder_lines,
    }


def _build_results(
    open_ended_lines: Set,
    disconnected_lines: Set,
    unserved_ecs: Set,
    missing_lv_feeder_ecs: Set,
    no_load_transformers: Set,
    phase_mismatch_features: List,
    missing_phase_lines: Set,
    zero_length_lines: Set,
    missing_impedance_lines: Set,
    missing_rating_transformers: Set,
    missing_impedance_transformers: Set,
    missing_normal_state_switches: Set,
    loop_lines: Set,
    switch_terminal_issues: Set,
    long_service_ecs: Set,
    ec_to_distance: Dict[str, float],
    very_long_lines: Set,
) -> Tuple[List, List[str], Set[str]]:
    results = []
    detected_tests: List[str] = []
    used_style_ids: Set[str] = set()

    def add_result(name: str, result):
        if result is None:
            return
        results.append(result)
        detected_tests.append(name)
        used_style_ids.update(result.geo_json_overlay.styles)

    add_result(
        "Open-ended line segments",
        cg._build_line_result(
            "Open-ended line segments",
            list(open_ended_lines),
            style_ids=["dq-open-ended-lines"],
            issue="open_end",
        ),
    )
    add_result(
        "Disconnected island lines",
        cg._build_line_result(
            "Disconnected island lines",
            list(disconnected_lines),
            style_ids=["dq-disconnected-lines"],
            issue="disconnected_island",
        ),
    )
    add_result(
        "Unserved EnergyConsumers",
        cm._build_point_result(
            "Unserved EnergyConsumers",
            list(unserved_ecs),
            style_ids=["dq-unserved-ec"],
            issue="unserved",
        ),
    )
    add_result(
        "EnergyConsumers missing LV feeder container",
        cm._build_point_result(
            "EnergyConsumers missing LV feeder container",
            list(missing_lv_feeder_ecs),
            style_ids=["dq-missing-lv-feeder-ec"],
            issue="missing_lv_feeder",
        ),
    )
    add_result(
        "Transformers with no downstream consumers",
        cm._build_transformer_result(
            "Transformers with no downstream consumers",
            list(no_load_transformers),
            style_ids=["dq-no-load-transformer"],
            issue="no_downstream_consumers",
        ),
    )
    add_result(
        "Phase mismatch at nodes",
        pc._build_feature_result(
            "Phase mismatch at nodes",
            phase_mismatch_features,
            style_ids=["dq-phase-mismatch-node"],
        ),
    )
    add_result(
        "Lines missing phase information",
        pc._build_line_result(
            "Lines missing phase information",
            list(missing_phase_lines),
            style_ids=["dq-missing-phase-lines"],
        ),
    )
    add_result(
        "Zero-length line segments",
        aa._build_line_result(
            "Zero-length line segments",
            list(zero_length_lines),
            style_ids=["dq-zero-length-lines"],
            issue="zero_length",
        ),
    )
    add_result(
        "Line segments missing impedance info",
        aa._build_line_result(
            "Line segments missing impedance info",
            list(missing_impedance_lines),
            style_ids=["dq-missing-impedance-lines"],
            issue="missing_impedance",
        ),
    )
    add_result(
        "Transformers missing rating",
        aa._build_transformer_result(
            "Transformers missing rating",
            list(missing_rating_transformers),
            style_ids=["dq-missing-rating-transformer"],
            issue="missing_rating",
        ),
    )
    add_result(
        "Transformers missing impedance",
        aa._build_transformer_result(
            "Transformers missing impedance",
            list(missing_impedance_transformers),
            style_ids=["dq-missing-impedance-transformer"],
            issue="missing_transformer_impedance",
        ),
    )
    add_result(
        "Switches missing normal state",
        aa._build_switch_result(
            "Switches missing normal state",
            list(missing_normal_state_switches),
            style_ids=["dq-missing-normal-state-switch"],
            issue="missing_normal_state",
        ),
    )
    add_result(
        "Loops without switches",
        pd._build_line_result(
            "Loops without switches",
            list(loop_lines),
            style_ids=["dq-loop-no-switch"],
            issue="loop_without_switch",
        ),
    )
    add_result(
        "Switch terminal anomalies",
        pd._build_switch_result(
            "Switch terminal anomalies",
            list(switch_terminal_issues),
            style_ids=["dq-switch-terminal-issue"],
            issue="switch_terminal_issue",
        ),
    )
    add_result(
        "Long service drops",
        sl._build_ec_distance_result(
            "Long service drops",
            list(long_service_ecs),
            ec_to_distance,
            style_ids=["dq-long-service-drop"],
        ),
    )
    add_result(
        "Very long line segments",
        sl._build_line_result(
            "Very long line segments",
            list(very_long_lines),
            style_ids=["dq-very-long-lines"],
            issue="very_long_line",
        ),
    )

    return results, detected_tests, used_style_ids


def _load_styles(used_style_ids: Set[str]) -> List[Dict]:
    style_dir = Path(__file__).resolve().parent
    style_files = [
        style_dir / "style_connectivity_gaps.json",
        style_dir / "style_consumer_mapping.json",
        style_dir / "style_phase_conductor.json",
        style_dir / "style_asset_attribute.json",
        style_dir / "style_protection_directionality.json",
        style_dir / "style_spatial_location.json",
    ]
    style_map: Dict[str, Dict] = {}
    for style_file in style_files:
        styles = json.load(open(style_file, "r"))
        for style in styles:
            if style.get("id") in used_style_ids:
                style_map[style["id"]] = style
    return list(style_map.values())


def _description_from_tests(detected_tests: List[str]) -> str:
    if not detected_tests:
        return "No anomalies detected."
    return "Detected anomalies: " + ", ".join(detected_tests)


def _slugify(text: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in text).strip("_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned


if __name__ == "__main__":
    asyncio.run(main())
