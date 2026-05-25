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
from zepben.eas import EasClient, Mutation
from zepben.eas import GeoJsonOverlayInput, StudyInput, StudyResultInput
from zepben.ewb import (
    AcLineSegment,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    PowerTransformer,
    Switch,
    connect_with_token,
)

from dq_utils import (
    chunk,
    get_zone_mrids,
    line_length_m,
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

    zero_length_lines: Set[AcLineSegment] = set()
    missing_impedance_lines: Set[AcLineSegment] = set()
    missing_rating_transformers: Set[PowerTransformer] = set()
    missing_impedance_transformers: Set[PowerTransformer] = set()
    missing_normal_state_switches: Set[Switch] = set()

    for feeders in chunk(feeder_mrids, 3):
        rpc_channel = _connect_rpc(config)
        for feeder_mrid in feeders:
            result = await _fetch_asset_inconsistencies(feeder_mrid, rpc_channel)
            if result is None:
                continue
            z_lines, i_lines, t_missing, t_imp_missing, s_missing = result
            zero_length_lines.update(z_lines)
            missing_impedance_lines.update(i_lines)
            missing_rating_transformers.update(t_missing)
            missing_impedance_transformers.update(t_imp_missing)
            missing_normal_state_switches.update(s_missing)

    style_path = Path(__file__).resolve().parent / "style_asset_attribute.json"
    styles = json.load(open(style_path, "r"))
    result_specs = [
        (
            "Zero-length line segments",
            _build_line_result(
                "Zero-length line segments",
                list(zero_length_lines),
                style_ids=["dq-zero-length-lines"],
                issue="zero_length",
            ),
        ),
        (
            "Line segments missing impedance info",
            _build_line_result(
                "Line segments missing impedance info",
                list(missing_impedance_lines),
                style_ids=["dq-missing-impedance-lines"],
                issue="missing_impedance",
            ),
        ),
        (
            "Transformers missing rating",
            _build_transformer_result(
                "Transformers missing rating",
                list(missing_rating_transformers),
                style_ids=["dq-missing-rating-transformer"],
                issue="missing_rating",
            ),
        ),
        (
            "Transformers missing impedance",
            _build_transformer_result(
                "Transformers missing impedance",
                list(missing_impedance_transformers),
                style_ids=["dq-missing-impedance-transformer"],
                issue="missing_transformer_impedance",
            ),
        ),
        (
            "Switches missing normal state",
            _build_switch_result(
                "Switches missing normal state",
                list(missing_normal_state_switches),
                style_ids=["dq-missing-normal-state-switch"],
                issue="missing_normal_state",
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

    eas_client = EasClient(host=config["host"], port=config["rpc_port"], protocol="https", access_token=config["access_token"], enable_legacy_methods=True,
                           asynchronous=True)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=f"Asset attribute inconsistencies ({', '.join(zone_mrids)})",
            description="Lines with missing length/impedance, transformers missing ratings/impedance, and switches missing normal state.",
            tags=["dq_asset_attributes", "-".join(zone_mrids)],
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


async def _fetch_asset_inconsistencies(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[Set[AcLineSegment], Set[AcLineSegment], Set[PowerTransformer], Set[PowerTransformer], Set[Switch]] | None:
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
) -> Tuple[Set[AcLineSegment], Set[AcLineSegment], Set[PowerTransformer], Set[PowerTransformer], Set[Switch]]:
    zero_length_lines = set()
    missing_impedance_lines = set()
    for line in network.objects(AcLineSegment):
        if line_length_m(line) <= 0.0:
            zero_length_lines.add(line)
        if not _has_impedance(line):
            missing_impedance_lines.add(line)

    missing_rating_transformers = set()
    missing_impedance_transformers = set()
    for pt in network.objects(PowerTransformer):
        if not _has_transformer_rating(pt):
            missing_rating_transformers.add(pt)
        if not _has_transformer_impedance(pt):
            missing_impedance_transformers.add(pt)

    missing_normal_state_switches = set()
    for sw in network.objects(Switch):
        if _normal_open_value(sw) is None:
            missing_normal_state_switches.add(sw)

    return zero_length_lines, missing_impedance_lines, missing_rating_transformers, missing_impedance_transformers, missing_normal_state_switches


def _has_impedance(line: AcLineSegment) -> bool:
    impedance_attrs = [
        "per_length_sequence_impedance",
        "per_length_phase_impedance",
        "per_length_impedance",
        "wire_info",
        "ac_line_segment_wire_info",
    ]
    for attr in impedance_attrs:
        value = getattr(line, attr, None)
        if value is not None:
            return True
    for attr in ["r", "x", "r0", "x0"]:
        value = getattr(line, attr, None)
        if value not in (None, 0):
            return True
    return False


def _has_transformer_rating(pt: PowerTransformer) -> bool:
    ends = list(pt.ends)
    for end in ends:
        rated_s = getattr(end, "rated_s", None)
        if rated_s and rated_s > 0:
            return True
        for rating in getattr(end, "s_ratings", []):
            if rating and getattr(rating, "rated_s", None):
                return True
    return False


def _has_transformer_impedance(pt: PowerTransformer) -> bool:
    primary_end = _primary_transformer_end(pt)
    if primary_end is None:
        return False

    for attr in ("r", "x", "r0", "x0"):
        value = getattr(primary_end, attr, None)
        if value not in (None, 0):
            return True

    star_impedance = getattr(primary_end, "star_impedance", None)
    if star_impedance is not None:
        rr = star_impedance.resistance_reactance()
        if rr is not None and not rr.is_empty():
            return True

    power_transformer_info = pt.asset_info
    if power_transformer_info is not None and primary_end.end_number is not None:
        rr = power_transformer_info.resistance_reactance(primary_end.end_number)
        if rr is not None and not rr.is_empty():
            return True

    return False


def _primary_transformer_end(pt: PowerTransformer):
    ends = list(pt.ends)
    if not ends:
        return None
    for end in ends:
        if getattr(end, "end_number", None) == 1:
            return end
    return max(ends, key=_end_voltage)


def _end_voltage(end) -> float:
    try:
        nominal = end.nominal_voltage
    except Exception:
        nominal = getattr(end, "rated_u", None)
    if nominal is None:
        base = getattr(end, "base_voltage", None)
        nominal = getattr(base, "nominal_voltage", None) if base is not None else None
    return float(nominal or 0.0)


def _normal_open_value(sw: Switch):
    if hasattr(sw, "is_normally_open"):
        try:
            return sw.is_normally_open()
        except Exception:
            return None
    for attr in ["normally_open", "normal_open"]:
        if hasattr(sw, attr):
            return getattr(sw, attr)
    return None


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


def _build_transformer_result(
    name: str,
    transformers: List[PowerTransformer],
    style_ids: List[str],
    issue: str,
) -> StudyResultInput | None:
    if not transformers:
        return None
    class_to_properties = {
        PowerTransformer: {
            "issue": lambda _: issue,
            "name": lambda pt: pt.name or pt.mrid,
            "type": lambda _: "pt",
        }
    }
    feature_collection: FeatureCollection = to_geojson_feature_collection(transformers, class_to_properties)
    if not feature_collection.features:
        return None
    return StudyResultInput(
        name=name,
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
