#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import argparse
import asyncio
import csv
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from itertools import islice
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union

from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry, LineString, Point
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import GeoJsonOverlay, Result, Study
from zepben.ewb import (
    EquipmentContainer,
    Feeder,
    IncludedEnergizedContainers,
    Location,
    NetworkConsumerClient,
    PowerSystemResource,
    Switch,
    connect_with_token,
)


DEFAULT_CONFIG_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "config.json"))
DEFAULT_BATCH_SIZE = 3
CSV_FIELDNAMES = [
    "switch_mrid",
    "name",
    "type",
    "tie_class",
    "confidence",
    "confidence_score",
    "feeder_count",
    "feeder_ids",
    "switch_feeder_ids",
    "adjacent_feeder_ids",
    "normal_feeder_ids",
    "current_feeder_ids",
    "lv_parent_feeder_ids",
    "source_feeder_ids",
    "source_feeder_count",
    "terminal_count",
    "connected_terminal_count",
    "terminals_missing_connectivity_node",
    "unresolved_container_ref_count",
    "unresolved_container_refs",
    "has_external_adjacent_feeder",
    "has_partial_terminal_signal",
    "candidate_signals",
    "is_open",
    "is_normally_open",
    "include_lv_mode",
    "scope_mode",
    "scope_label",
    "generated_at_utc",
]


@dataclass
class SwitchEvidence:
    switch: Switch
    source_feeder_mrids: Set[str] = field(default_factory=set)
    unresolved_container_refs: Set[str] = field(default_factory=set)


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


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


async def main():
    (
        zone_mrids,
        feeder_mrids,
        mode,
        config_path,
        include_lv,
        full_network_list,
        include_candidates_in_study,
        csv_output,
    ) = _parse_args(sys.argv[1:])
    with open(config_path, "r") as f:
        config = json.loads(f.read())

    print(f"Start time: {datetime.now()}")

    rpc_channel = _connect_rpc(config)

    if mode in ("zones", "full-network"):
        client = NetworkConsumerClient(rpc_channel)
        hierarchy = (await client.get_network_hierarchy()).throw_on_error()
        substations = hierarchy.value.substations

        if mode == "zones":
            feeder_mrids = _collect_feeder_mrids(substations, zone_mrids)
            print(f"Collecting feeders from zones {', '.join(zone_mrids)}.")
        else:
            if full_network_list == "zones":
                zone_mrids = _collect_all_zone_mrids(substations)
                feeder_mrids = _collect_feeder_mrids(substations, zone_mrids)
                print(
                    "Full-network mode (zones): "
                    f"resolved {len(zone_mrids)} zones and {len(feeder_mrids)} feeders."
                )
            else:
                feeder_mrids = _collect_all_feeder_mrids(substations)
                print(f"Full-network mode (feeders): resolved {len(feeder_mrids)} feeders.")
    else:
        print(f"Running for feeders {', '.join(feeder_mrids)}.")

    if not feeder_mrids:
        print("No feeders resolved for the requested scope. Nothing to process.")
        return

    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")
    print(f"LV inclusion: {'enabled' if include_lv else 'disabled (MV-only)'}")
    print(
        "Candidate study results: "
        f"{'enabled' if include_candidates_in_study else 'disabled (confirmed-only default)'}"
    )

    switch_evidence_by_mrid: Dict[str, SwitchEvidence] = {}
    failed_feeder_requests: List[str] = []

    for feeders in chunk(feeder_mrids, DEFAULT_BATCH_SIZE):
        rpc_channel = _connect_rpc(config)
        print(f"Processing feeders {', '.join(feeders)}")
        futures_by_feeder = {
            feeder_mrid: asyncio.ensure_future(
                _fetch_feeder_switch_evidence(
                    feeder_mrid=feeder_mrid,
                    rpc_channel=rpc_channel,
                    include_lv=include_lv,
                )
            )
            for feeder_mrid in feeders
        }

        for feeder_mrid, future in futures_by_feeder.items():
            try:
                feeder_switches = await future
            except Exception as error:
                print(f"Failed feeder {feeder_mrid}: {error}")
                failed_feeder_requests.append(feeder_mrid)
                continue

            if feeder_switches is None:
                failed_feeder_requests.append(feeder_mrid)
                continue

            _merge_switch_evidence(switch_evidence_by_mrid, feeder_switches)

    failed_feeder_request_count = len(failed_feeder_requests)
    succeeded_feeder_request_count = len(feeder_mrids) - failed_feeder_request_count
    failed_feeder_mrids = sorted(set(failed_feeder_requests))

    print(
        "Feeder fetch summary: "
        f"succeeded={succeeded_feeder_request_count}, "
        f"failed={failed_feeder_request_count}, "
        f"total={len(feeder_mrids)}"
    )
    if failed_feeder_mrids:
        print(
            f"Failed feeder MRIDs ({len(failed_feeder_mrids)} unique): "
            f"{', '.join(failed_feeder_mrids)}"
        )

    print(f"Unique switches considered: {len(switch_evidence_by_mrid)}")

    switch_to_properties: Dict[str, Dict[str, Any]] = {}
    confirmed_switches: List[Switch] = []
    candidate_switches: List[Switch] = []

    confirmed_count = 0
    candidate_count = 0

    for evidence in switch_evidence_by_mrid.values():
        props = _build_tie_properties(evidence, include_lv)
        tie_class = props.get("tie_class")
        if tie_class == "confirmed_tie":
            confirmed_count += 1
            confirmed_switches.append(evidence.switch)
        elif tie_class == "candidate_tie":
            candidate_count += 1
            candidate_switches.append(evidence.switch)
        else:
            continue

        switch_to_properties[evidence.switch.mrid] = props

    print(
        "Detected tie switches: "
        f"confirmed={confirmed_count}, candidate={candidate_count}, total={confirmed_count + candidate_count}"
    )

    if mode == "zones":
        scope_label = ", ".join(zone_mrids)
        scope_tag = "-".join(zone_mrids)
    elif mode == "feeders":
        scope_label = ", ".join(feeder_mrids)
        scope_tag = "-".join(feeder_mrids)
    else:
        scope_label = f"full-network ({full_network_list})"
        scope_tag = f"full-network-{full_network_list}"

    lv_tag = "mv_lv" if include_lv else "mv_only"
    candidate_result_tag = (
        "candidate_results_included" if include_candidates_in_study else "candidate_results_excluded"
    )
    csv_report_path = _resolve_csv_report_path(csv_output, scope_tag, lv_tag)
    csv_rows = _build_csv_rows(switch_to_properties, mode, scope_label)
    _write_tie_csv_report(csv_report_path, csv_rows)
    print(f"Wrote tie switch CSV report: {csv_report_path} ({len(csv_rows)} rows)")

    confirmed_feature_collection = _build_feature_collection(confirmed_switches, switch_to_properties)
    candidate_feature_collection = _build_feature_collection(candidate_switches, switch_to_properties)
    if not confirmed_feature_collection.features and not candidate_feature_collection.features:
        print("No mappable tie switch features were found (e.g. missing location). Study upload skipped.")
        return

    print(
        "Mappable tie features: "
        f"confirmed={len(confirmed_feature_collection.features)}, "
        f"candidate={len(candidate_feature_collection.features)}, "
        f"total={len(confirmed_feature_collection.features) + len(candidate_feature_collection.features)}"
    )

    style_path = os.path.join(os.path.dirname(__file__), "style_feeder_tie_switches.json")
    with open(style_path, "r") as f:
        styles = json.load(f)

    fetch_coverage_summary = (
        "Feeder fetch coverage: "
        f"succeeded={succeeded_feeder_request_count}, "
        f"failed={failed_feeder_request_count}, "
        f"total={len(feeder_mrids)}, "
        f"failed_unique={len(failed_feeder_mrids)}."
    )
    candidate_upload_summary = (
        "Candidate ties are included in the uploaded study."
        if include_candidates_in_study
        else "Candidate ties are excluded from the uploaded study by default "
             "(use --include-candidates-in-study to include them)."
    )

    results: List[Result] = []
    if confirmed_feature_collection.features:
        results.append(
            Result(
                name=f"Confirmed feeder ties ({len(confirmed_feature_collection.features)})",
                geo_json_overlay=GeoJsonOverlay(
                    data=confirmed_feature_collection,
                    styles=["feeder-tie-confirmed", "feeder-tie-confirmed-label"],
                ),
            )
        )
    if candidate_feature_collection.features and include_candidates_in_study:
        results.append(
            Result(
                name=f"Candidate feeder ties ({len(candidate_feature_collection.features)})",
                geo_json_overlay=GeoJsonOverlay(
                    data=candidate_feature_collection,
                    styles=["feeder-tie-candidate", "feeder-tie-candidate-label"],
                ),
            )
        )
    elif candidate_feature_collection.features:
        print(
            "Candidate tie features detected but excluded from study upload by default. "
            "Use --include-candidates-in-study to include them in EAS results."
        )

    if not results:
        print("No enabled mappable tie switch layers were produced. Study upload skipped.")
        return

    eas_client = EasClient(
        host=config["host"],
        port=config["rpc_port"],
        protocol="https",
        access_token=config["access_token"],
    )
    print(f"Uploading Study for {mode} {scope_label} ...")
    await eas_client.async_upload_study(
        Study(
            name=f"Feeder tie switches ({scope_label})",
            description=(
                "Detects feeder tie switches by feeder-membership evidence. "
                "Detected tie switch attributes are exported to CSV. "
                "Confirmed ties are uploaded by default. "
                f"{candidate_upload_summary} "
                f"{fetch_coverage_summary}"
            ),
            tags=[
                "feeder_tie_switches",
                lv_tag,
                scope_tag,
                candidate_result_tag,
                f"fetch_failed_{failed_feeder_request_count}",
                f"fetch_total_{len(feeder_mrids)}",
            ],
            results=results,
            styles=styles,
        )
    )
    await eas_client.aclose()
    print("Uploaded Study")
    print(f"Finish time: {datetime.now()}")


def _collect_feeder_mrids(substations: Dict, zone_mrids: List[str]) -> List[str]:
    feeder_mrids: List[str] = []
    for zone_mrid in zone_mrids:
        if zone_mrid in substations:
            for feeder in substations[zone_mrid].feeders:
                feeder_mrids.append(feeder.mrid)
    return feeder_mrids


def _collect_all_zone_mrids(substations: Dict) -> List[str]:
    return sorted(zone_mrid for zone_mrid in substations.keys())


def _collect_all_feeder_mrids(substations: Dict) -> List[str]:
    feeder_mrids: List[str] = []
    seen: Set[str] = set()
    for zone_mrid in _collect_all_zone_mrids(substations):
        for feeder in substations[zone_mrid].feeders:
            if feeder.mrid in seen:
                continue
            seen.add(feeder.mrid)
            feeder_mrids.append(feeder.mrid)
    return feeder_mrids


async def _fetch_feeder_switch_evidence(
    feeder_mrid: str,
    rpc_channel,
    include_lv: bool,
) -> Optional[Dict[str, SwitchEvidence]]:
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)

    if include_lv:
        result = await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
        )
    else:
        result = await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
        )
    if result.was_failure:
        print(f"Failed feeder {feeder_mrid}: {_failure_reason(result)}")
        return None

    network = client.service
    print(f"Finished fetching Feeder {feeder_mrid}")

    feeder_switches: Dict[str, SwitchEvidence] = {}
    for sw in network.objects(Switch):
        if not include_lv and _is_lv_switch(sw):
            continue

        feeder_switches[sw.mrid] = SwitchEvidence(
            switch=sw,
            source_feeder_mrids={feeder_mrid},
            unresolved_container_refs=_unresolved_container_refs(network, sw.mrid),
        )

    return feeder_switches


def _failure_reason(result: Any) -> str:
    thrown = getattr(result, "thrown", None)
    if thrown is not None:
        text = str(thrown).strip()
        if text and text.lower() != "none":
            return text

    parts: List[str] = []
    for attr in ("message", "details", "status", "code", "error"):
        value = getattr(result, attr, None)
        if value is None:
            continue
        text = str(value).strip()
        if text and text.lower() != "none":
            parts.append(f"{attr}={text}")

    if parts:
        return "; ".join(parts)

    return repr(result)


def _merge_switch_evidence(
    combined: Dict[str, SwitchEvidence],
    next_batch: Dict[str, SwitchEvidence],
):
    for mrid, evidence in next_batch.items():
        existing = combined.get(mrid)
        if existing is None:
            combined[mrid] = evidence
            continue

        existing.source_feeder_mrids.update(evidence.source_feeder_mrids)
        existing.unresolved_container_refs.update(evidence.unresolved_container_refs)

        # Keep whichever object has location data for better map output.
        if existing.switch.location is None and evidence.switch.location is not None:
            existing.switch = evidence.switch


def _build_tie_properties(evidence: SwitchEvidence, include_lv: bool) -> Dict[str, Any]:
    switch = evidence.switch
    switch_feeder_ids, normal_feeder_ids, current_feeder_ids, lv_parent_feeder_ids = _switch_feeder_ids(switch, include_lv)
    adjacent_feeder_ids, connected_terminal_count, missing_connectivity_node_count = _adjacent_feeder_ids(switch, include_lv)
    combined_feeder_ids = switch_feeder_ids | adjacent_feeder_ids

    unresolved_ref_count = len(evidence.unresolved_container_refs)
    total_terminal_count = len(list(switch.terminals))
    is_open = _switch_open_value(switch)
    is_normally_open = _switch_normal_open_value(switch)

    has_external_adjacent_feeder = bool(adjacent_feeder_ids - switch_feeder_ids)
    has_partial_terminal_signal = connected_terminal_count < 2 or missing_connectivity_node_count > 0

    tie_class = "non_tie"
    confidence = "none"
    confidence_score = 0
    candidate_signals: List[str] = []

    if len(combined_feeder_ids) >= 2:
        tie_class = "confirmed_tie"
        confidence = "high"
        confidence_score = 100
    elif len(combined_feeder_ids) == 1:
        if unresolved_ref_count > 0:
            candidate_signals.append("unresolved_container_ref")
        if has_external_adjacent_feeder:
            candidate_signals.append("external_adjacent_feeder")
        if has_partial_terminal_signal:
            candidate_signals.append("partial_terminal_connectivity")
        if is_open is True or is_normally_open is True:
            candidate_signals.append("open_state")

        if candidate_signals:
            tie_class = "candidate_tie"
            if "external_adjacent_feeder" in candidate_signals or "unresolved_container_ref" in candidate_signals:
                confidence = "medium"
                confidence_score = 70
            else:
                confidence = "low"
                confidence_score = 50

    return {
        "name": switch.name or switch.mrid,
        "type": "switch",
        "tie_class": tie_class,
        "confidence": confidence,
        "confidence_score": confidence_score,
        "feeder_count": len(combined_feeder_ids),
        "feeder_ids": sorted(combined_feeder_ids),
        "switch_feeder_ids": sorted(switch_feeder_ids),
        "adjacent_feeder_ids": sorted(adjacent_feeder_ids),
        "normal_feeder_ids": sorted(normal_feeder_ids),
        "current_feeder_ids": sorted(current_feeder_ids),
        "lv_parent_feeder_ids": sorted(lv_parent_feeder_ids),
        "source_feeder_ids": sorted(evidence.source_feeder_mrids),
        "source_feeder_count": len(evidence.source_feeder_mrids),
        "terminal_count": total_terminal_count,
        "connected_terminal_count": connected_terminal_count,
        "terminals_missing_connectivity_node": missing_connectivity_node_count,
        "unresolved_container_ref_count": unresolved_ref_count,
        "unresolved_container_refs": sorted(evidence.unresolved_container_refs),
        "has_external_adjacent_feeder": has_external_adjacent_feeder,
        "has_partial_terminal_signal": has_partial_terminal_signal,
        "candidate_signals": candidate_signals,
        "is_open": is_open,
        "is_normally_open": is_normally_open,
        "include_lv_mode": include_lv,
    }


def _switch_feeder_ids(switch: Switch, include_lv: bool) -> Tuple[Set[str], Set[str], Set[str], Set[str]]:
    normal_feeder_ids = _normal_feeder_ids(switch)
    current_feeder_ids = _current_feeder_ids(switch)
    feeder_ids = set(normal_feeder_ids or current_feeder_ids)

    lv_parent_feeder_ids: Set[str] = set()
    if include_lv:
        lv_parent_feeder_ids.update(_normal_lv_parent_feeder_ids(switch))
        if not lv_parent_feeder_ids:
            lv_parent_feeder_ids.update(_current_lv_parent_feeder_ids(switch))
        feeder_ids.update(lv_parent_feeder_ids)

    return feeder_ids, normal_feeder_ids, current_feeder_ids, lv_parent_feeder_ids


def _equipment_feeder_ids(equipment: Any, include_lv: bool) -> Set[str]:
    normal_feeder_ids = _normal_feeder_ids(equipment)
    current_feeder_ids = _current_feeder_ids(equipment)
    feeder_ids = set(normal_feeder_ids or current_feeder_ids)

    if include_lv:
        lv_parent_ids = _normal_lv_parent_feeder_ids(equipment)
        if not lv_parent_ids:
            lv_parent_ids = _current_lv_parent_feeder_ids(equipment)
        feeder_ids.update(lv_parent_ids)

    return feeder_ids


def _adjacent_feeder_ids(switch: Switch, include_lv: bool) -> Tuple[Set[str], int, int]:
    feeder_ids: Set[str] = set()
    connected_terminal_count = 0
    missing_connectivity_node_count = 0

    for terminal in switch.terminals:
        node = terminal.connectivity_node
        if node is None:
            missing_connectivity_node_count += 1
            continue

        connected_terminal_count += 1
        for other_terminal in node.terminals:
            conducting_equipment = other_terminal.conducting_equipment
            if conducting_equipment is None or conducting_equipment is switch:
                continue
            feeder_ids.update(_equipment_feeder_ids(conducting_equipment, include_lv))

    return feeder_ids, connected_terminal_count, missing_connectivity_node_count


def _normal_feeder_ids(equipment: Any) -> Set[str]:
    return {
        feeder.mrid
        for feeder in (getattr(equipment, "normal_feeders", None) or [])
        if getattr(feeder, "mrid", None)
    }


def _current_feeder_ids(equipment: Any) -> Set[str]:
    return {
        feeder.mrid
        for feeder in (getattr(equipment, "current_feeders", None) or [])
        if getattr(feeder, "mrid", None)
    }


def _normal_lv_parent_feeder_ids(equipment: Any) -> Set[str]:
    parent_ids: Set[str] = set()
    for lv_feeder in (getattr(equipment, "normal_lv_feeders", None) or []):
        for feeder in (getattr(lv_feeder, "normal_energizing_feeders", None) or []):
            if getattr(feeder, "mrid", None):
                parent_ids.add(feeder.mrid)
    return parent_ids


def _current_lv_parent_feeder_ids(equipment: Any) -> Set[str]:
    parent_ids: Set[str] = set()
    for lv_feeder in (getattr(equipment, "current_lv_feeders", None) or []):
        for feeder in (getattr(lv_feeder, "current_energizing_feeders", None) or []):
            if getattr(feeder, "mrid", None):
                parent_ids.add(feeder.mrid)
    return parent_ids


def _switch_open_value(sw: Switch) -> Optional[bool]:
    if hasattr(sw, "is_open"):
        try:
            return bool(sw.is_open())
        except Exception:
            pass
    if hasattr(sw, "open"):
        try:
            value = getattr(sw, "open")
            return bool(value() if callable(value) else value)
        except Exception:
            pass
    return None


def _switch_normal_open_value(sw: Switch) -> Optional[bool]:
    if hasattr(sw, "is_normally_open"):
        try:
            return bool(sw.is_normally_open())
        except Exception:
            pass
    for attr in ("normally_open", "normal_open"):
        if hasattr(sw, attr):
            try:
                value = getattr(sw, attr)
                return bool(value() if callable(value) else value)
            except Exception:
                continue
    return None


def _is_lv_switch(sw: Switch) -> bool:
    base_voltage = getattr(sw, "base_voltage_value", None)
    if base_voltage is not None and base_voltage > 0:
        return base_voltage <= 1000

    # Fallback for models where base voltage is absent:
    # if the switch appears only in LV containers, treat as LV.
    has_mv = bool(_normal_feeder_ids(sw) or _current_feeder_ids(sw))
    has_lv = bool(_normal_lv_parent_feeder_ids(sw) or _current_lv_parent_feeder_ids(sw))
    return has_lv and not has_mv


def _unresolved_container_refs(network, mrid: str) -> Set[str]:
    get_refs = getattr(network, "get_unresolved_references_from", None)
    if not callable(get_refs):
        return set()

    refs: Set[str] = set()
    try:
        for ref in get_refs(mrid):
            resolver = getattr(ref, "resolver", None)
            to_class = getattr(resolver, "to_class", None)
            to_mrid = getattr(ref, "to_mrid", None)
            if not to_mrid:
                continue

            if to_class is EquipmentContainer or getattr(to_class, "__name__", None) == "EquipmentContainer":
                refs.add(str(to_mrid))
    except Exception:
        return refs

    return refs


def _build_feature_collection(
    switches: List[Switch],
    switch_to_properties: Dict[str, Dict[str, Any]],
) -> FeatureCollection:
    features: List[Feature] = []
    for sw in switches:
        geometry = to_geojson_geometry(sw.location)
        if geometry is None:
            continue
        features.append(Feature(sw.mrid, geometry, switch_to_properties.get(sw.mrid, {})))
    return FeatureCollection(features)


def to_geojson_feature_collection(
    psrs: List[PowerSystemResource],
    class_to_properties: Dict[type, Dict[str, Any]],
) -> FeatureCollection:
    # Retained for compatibility with existing study patterns if needed for extension.
    features = []
    for psr in psrs:
        properties = class_to_properties.get(type(psr))
        if properties is None:
            continue
        geometry = to_geojson_geometry(psr.location)
        if geometry is None:
            continue
        features.append(Feature(psr.mrid, geometry, {k: f(psr) for (k, f) in properties.items()}))
    return FeatureCollection(features)


def to_geojson_geometry(location: Location) -> Union[Geometry, None]:
    points = list(location.points) if location is not None else []
    if len(points) > 1:
        return LineString([(point.x_position, point.y_position) for point in points])
    if len(points) == 1:
        return Point((points[0].x_position, points[0].y_position))
    return None


def _resolve_csv_report_path(csv_output: str, scope_tag: str, lv_tag: str) -> str:
    if csv_output:
        return csv_output
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(
        os.path.dirname(__file__),
        f"feeder_tie_switches_{scope_tag}_{lv_tag}_{timestamp}.csv",
    )


def _build_csv_rows(
    switch_to_properties: Dict[str, Dict[str, Any]],
    mode: str,
    scope_label: str,
) -> List[Dict[str, Any]]:
    generated_at_utc = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows: List[Dict[str, Any]] = []
    for switch_mrid in sorted(switch_to_properties.keys()):
        row: Dict[str, Any] = {
            "switch_mrid": switch_mrid,
            "scope_mode": mode,
            "scope_label": scope_label,
            "generated_at_utc": generated_at_utc,
        }
        row.update(switch_to_properties[switch_mrid])
        rows.append(row)
    return rows


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, set)):
        return "|".join(str(item) for item in value)
    return str(value)


def _write_tie_csv_report(path: str, rows: List[Dict[str, Any]]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_cell(row.get(column)) for column in CSV_FIELDNAMES})


def _parse_args(argv: List[str]) -> Tuple[List[str], List[str], str, str, bool, str, bool, str]:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a feeder-tie switch study for one or more zones or feeders. "
            "Default behavior is MV-only with confirmed ties only in study results; "
            "use --include-lv to include LV containers."
        )
    )
    parser.add_argument(
        "--mode",
        choices=["zones", "feeders", "full-network"],
        default="zones",
        help="Run by zones, feeders, or full-network list expansion (default: zones).",
    )
    parser.add_argument(
        "--zones",
        default="CPM",
        help="Comma-separated zone codes (default: CPM).",
    )
    parser.add_argument(
        "--feeders",
        default="",
        help="Comma-separated feeder MRIDs (used when --mode feeders).",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to config.json (default: {DEFAULT_CONFIG_PATH}).",
    )
    parser.add_argument(
        "--full-network-list",
        choices=["feeders", "zones"],
        default="feeders",
        help=(
            "When --mode full-network is used: run all feeders directly (feeders) "
            "or derive all zones and then expand to feeders (zones)."
        ),
    )
    parser.add_argument(
        "--include-lv",
        action="store_true",
        help="Include LV containers in fetch and tie detection (default is MV-only).",
    )
    parser.add_argument(
        "--include-candidates-in-study",
        action="store_true",
        help="Include candidate tie layers in uploaded study results (default is confirmed-only).",
    )
    parser.add_argument(
        "--csv-output",
        default="",
        help=(
            "Optional path for detected tie CSV report. "
            "Default writes feeder_tie_switches_<scope>_<mode>_<timestamp>.csv in this folder."
        ),
    )
    parser.add_argument(
        "ids",
        nargs="*",
        help="Zone codes or feeder MRIDs (positional values override --zones/--feeders).",
    )
    args = parser.parse_args(argv)

    def _split_values(values: Union[str, Sequence[str]]) -> List[str]:
        if isinstance(values, str):
            items = values.split(",")
        else:
            items = list(values)
        return [item.strip() for item in items if item and item.strip()]

    if args.mode == "feeders":
        feeders = _split_values(args.ids) or _split_values(args.feeders)
        if not feeders:
            raise ValueError("At least one feeder MRID is required in feeder mode.")
        return (
            [],
            feeders,
            args.mode,
            args.config,
            bool(args.include_lv),
            args.full_network_list,
            bool(args.include_candidates_in_study),
            args.csv_output,
        )

    if args.mode == "full-network":
        return (
            [],
            [],
            args.mode,
            args.config,
            bool(args.include_lv),
            args.full_network_list,
            bool(args.include_candidates_in_study),
            args.csv_output,
        )

    zones = _split_values(args.ids) or _split_values(args.zones)
    if not zones:
        raise ValueError("At least one zone code is required in zone mode.")

    return (
        zones,
        [],
        args.mode,
        args.config,
        bool(args.include_lv),
        args.full_network_list,
        bool(args.include_candidates_in_study),
        args.csv_output,
    )


if __name__ == "__main__":
    asyncio.run(main())
