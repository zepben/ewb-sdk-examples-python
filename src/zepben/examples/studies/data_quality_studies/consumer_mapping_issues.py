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
    EnergyConsumer,
    PowerTransformer,
    Feeder,
    IncludedEnergizedContainers,
    NetworkConsumerClient,
    NetworkTraceStep,
    PhaseCode,
    Tracing,
    connect_with_token,
    downstream,
    stop_at_open,
    upstream,
)
from zepben.ewb.services.network.tracing.networktrace.operators.network_state_operators import (
    NetworkStateOperators,
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

    unserved_ecs: Set[EnergyConsumer] = set()
    missing_lv_feeder_ecs: Set[EnergyConsumer] = set()
    no_load_transformers: Set[PowerTransformer] = set()

    for feeders in chunk(feeder_mrids, 3):
        rpc_channel = _connect_rpc(config)
        for feeder_mrid in feeders:
            result = await _fetch_consumer_mapping_issues(feeder_mrid, rpc_channel)
            if result is None:
                continue
            feeder_unserved, feeder_missing_lv, feeder_no_load = result
            unserved_ecs.update(feeder_unserved)
            missing_lv_feeder_ecs.update(feeder_missing_lv)
            no_load_transformers.update(feeder_no_load)

    style_path = Path(__file__).resolve().parent / "style_consumer_mapping.json"
    styles = json.load(open(style_path, "r"))
    result_specs = [
        (
            "Unserved EnergyConsumers",
            _build_point_result(
                "Unserved EnergyConsumers",
                list(unserved_ecs),
                style_ids=["dq-unserved-ec"],
                issue="unserved",
            ),
        ),
        (
            "EnergyConsumers missing LV feeder container",
            _build_point_result(
                "EnergyConsumers missing LV feeder container",
                list(missing_lv_feeder_ecs),
                style_ids=["dq-missing-lv-feeder-ec"],
                issue="missing_lv_feeder",
            ),
        ),
        (
            "Transformers with no downstream consumers",
            _build_transformer_result(
                "Transformers with no downstream consumers",
                list(no_load_transformers),
                style_ids=["dq-no-load-transformer"],
                issue="no_downstream_consumers",
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
            name=f"Consumer mapping issues ({', '.join(zone_mrids)})",
            description="Unserved consumers, consumers missing LV feeder containers, and transformers with no downstream consumers.",
            tags=["dq_consumer_mapping", "-".join(zone_mrids)],
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


async def _fetch_consumer_mapping_issues(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[Set[EnergyConsumer], Set[EnergyConsumer], Set[PowerTransformer]] | None:
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
) -> Tuple[Set[EnergyConsumer], Set[EnergyConsumer], Set[PowerTransformer]]:
    await Tracing.set_direction().run(network, network_state_operators=NetworkStateOperators.NORMAL)

    unserved_ecs: Set[EnergyConsumer] = set()
    missing_lv_feeder_ecs: Set[EnergyConsumer] = set()

    for ec in network.objects(EnergyConsumer):
        if not _has_lv_feeder_container(ec):
            missing_lv_feeder_ecs.add(ec)
        if not await _has_upstream_transformer(ec):
            unserved_ecs.add(ec)

    no_load_transformers: Set[PowerTransformer] = set()
    for pt in network.objects(PowerTransformer):
        if not _belongs_to_feeder(pt, feeder_mrid):
            continue
        downstream_eq = await _get_downstream_eq(pt)
        if not any(isinstance(eq, EnergyConsumer) for eq in downstream_eq):
            no_load_transformers.add(pt)

    return unserved_ecs, missing_lv_feeder_ecs, no_load_transformers


async def _has_upstream_transformer(ec: EnergyConsumer) -> bool:
    found = False

    async def mark_transformer(ps: NetworkTraceStep, _):
        nonlocal found
        if isinstance(ps.path.to_equipment, PowerTransformer):
            found = True

    await (
        Tracing.network_trace()
        .add_condition(upstream())
        .add_condition(stop_at_open())
        .add_step_action(mark_transformer)
        .add_stop_condition(_is_transformer)
    ).run(start=ec, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return found


def _is_transformer(ps: NetworkTraceStep, _context=None) -> bool:
    return isinstance(ps.path.to_equipment, PowerTransformer)


async def _get_downstream_eq(pt: PowerTransformer) -> Set[object]:
    nodes: Set[object] = {pt}
    adjacency: Dict[object, Set[object]] = {}

    async def collect_edges(ps: NetworkTraceStep, _):
        nodes.add(ps.path.from_equipment)
        nodes.add(ps.path.to_equipment)
        if ps.path.traced_externally:
            adjacency.setdefault(ps.path.from_equipment, set()).add(ps.path.to_equipment)

    await (
        Tracing.network_trace()
        .add_condition(downstream())
        .add_step_action(collect_edges)
    ).run(start=pt, phases=PhaseCode.ABCN, can_stop_on_start_item=False)

    return nodes


def _has_lv_feeder_container(ec: EnergyConsumer) -> bool:
    return any(True for _ in ec.normal_lv_feeders)


def _belongs_to_feeder(psr, feeder_mrid: str) -> bool:
    if any(feeder.mrid == feeder_mrid for feeder in psr.normal_feeders):
        return True
    for lv_feeder in psr.normal_lv_feeders:
        if any(feeder.mrid == feeder_mrid for feeder in lv_feeder.normal_energizing_feeders):
            return True
    return False


def _build_point_result(
    name: str,
    ecs: List[EnergyConsumer],
    style_ids: List[str],
    issue: str,
) -> Result | None:
    if not ecs:
        return None
    class_to_properties = {
        EnergyConsumer: {
            "issue": lambda _: issue,
            "name": lambda ec: ec.name or ec.mrid,
            "type": lambda _: "ec",
        }
    }
    feature_collection: FeatureCollection = to_geojson_feature_collection(ecs, class_to_properties)
    if not feature_collection.features:
        return None
    return Result(
        name=name,
        geo_json_overlay=GeoJsonOverlay(
            data=feature_collection,
            styles=style_ids,
        ),
    )


def _build_transformer_result(
    name: str,
    transformers: List[PowerTransformer],
    style_ids: List[str],
    issue: str,
) -> Result | None:
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
    return Result(
        name=name,
        geo_json_overlay=GeoJsonOverlay(
            data=feature_collection,
            styles=style_ids,
        ),
    )


if __name__ == "__main__":
    asyncio.run(main())
