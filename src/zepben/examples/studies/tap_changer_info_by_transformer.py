#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import asyncio
import json
from pathlib import Path
from datetime import datetime
from itertools import islice
from typing import List, Dict, Tuple, Callable, Any, Union, Type

from geojson import FeatureCollection, Feature
from geojson.geometry import Geometry, LineString, Point
from zepben.eas import EasClient, Mutation, StudyInput, StudyResultInput, GeoJsonOverlayInput
from zepben.examples.studies.study_utils import (
    create_eas_client_from_config,
    connect_rpc_from_config,
    load_examples_config,
)
from zepben.ewb import (
    PowerTransformer,
    RatioTapChanger,
    NetworkConsumerClient,
    Feeder,
    PowerSystemResource,
    Location,
    connect_with_token,
)


c = load_examples_config()

STYLE_PATH = Path(__file__).resolve().parent / "style_tap_changer.json"


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


async def main():
    # Only process feeders in the following zones
    zone_mrids = ["CPM"]
    print(f"Start time: {datetime.now()}")

    rpc_channel = connect_rpc_from_config(c)
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
    transformer_to_tap: Dict[str, Dict[str, Any]] = {}

    batches = chunk(feeder_mrids, 3)
    for feeders in batches:
        futures = []
        rpc_channel = connect_rpc_from_config(c)
        print(f"Processing feeders {', '.join(feeders)}")
        for feeder_mrid in feeders:
            futures.append(asyncio.ensure_future(fetch_tap_info_for_feeder(feeder_mrid, rpc_channel)))

        for future in futures:
            transformers, tap_info = await future
            if transformers:
                all_transformers.extend(transformers)
                transformer_to_tap.update(tap_info)

    print(f"Creating study for {len(all_transformers)} transformers")

    eas_client = create_eas_client_from_config(c)
    print(f"Uploading Study for zones {', '.join(zone_mrids)} ...")
    await upload_tap_changer_study(
        eas_client,
        all_transformers,
        transformer_to_tap,
        name=f"Tap changer info ({', '.join(zone_mrids)})",
        description="Tap changer info shown as JSON at transformers; step or normal_step shown as label.",
        tags=["tap_changer", "-".join(zone_mrids)],
        styles=json.loads(STYLE_PATH.read_text()),
    )
    await eas_client.close()
    print("Uploaded Study")

    print(f"Finish time: {datetime.now()}")


async def fetch_tap_info_for_feeder(
    feeder_mrid: str,
    rpc_channel,
) -> Tuple[List[PowerTransformer], Dict[str, Dict[str, Any]]]:
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)

    result = (
        await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=None,
        )
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return [], {}

    network = client.service
    print(f"Finished fetching Feeder {feeder_mrid}")

    transformers: List[PowerTransformer] = []
    transformer_to_tap: Dict[str, Dict[str, Any]] = {}
    tap_changers_by_transformer = _collect_tap_changers_by_transformer(network)

    for io in (pt for pt in network.objects(PowerTransformer)):
        pt: PowerTransformer = io
        tap_info = _tap_info_from_transformer(pt, tap_changers_by_transformer.get(pt.mrid, []))
        if tap_info is None:
            continue
        transformers.append(pt)
        transformer_to_tap[pt.mrid] = tap_info

    return transformers, transformer_to_tap


def _collect_tap_changers_by_transformer(network) -> Dict[str, List[RatioTapChanger]]:
    mapping: Dict[str, List[RatioTapChanger]] = {}
    for tc in network.objects(RatioTapChanger):
        end = tc.transformer_end
        if end is None or end.power_transformer is None:
            continue
        pt_mrid = end.power_transformer.mrid
        mapping.setdefault(pt_mrid, []).append(tc)
    return mapping


def _tap_info_from_transformer(
    pt: PowerTransformer,
    extra_tap_changers: List[RatioTapChanger],
) -> Union[Dict[str, Any], None]:
    tap_changers: List[RatioTapChanger] = []
    for end in pt.ends:
        if end.ratio_tap_changer is not None:
            tap_changers.append(end.ratio_tap_changer)

    tap_changers.extend(extra_tap_changers)
    if tap_changers:
        # De-duplicate by mRID
        unique = {}
        for tc in tap_changers:
            unique[tc.mrid] = tc
        tap_changers = list(unique.values())

    if not tap_changers:
        return None

    # Prefer a tap changer with an explicit step; fallback to any with normal_step
    preferred = None
    for tc in tap_changers:
        if tc.step is not None:
            preferred = tc
            break
    if preferred is None:
        for tc in tap_changers:
            if tc.normal_step is not None:
                preferred = tc
                break
    if preferred is None:
        preferred = tap_changers[0]

    step_value = preferred.step if preferred.step is not None else preferred.normal_step
    tap_label = f"{step_value}" if step_value is not None else "n/a"

    tap_json = {
        "step": preferred.step,
        "normal_step": preferred.normal_step,
        "neutral_step": preferred.neutral_step,
        "low_step": preferred.low_step,
        "high_step": preferred.high_step,
        "control_enabled": preferred.control_enabled,
    }

    return {
        "tap_label": tap_label,
        "tap_json": json.dumps(tap_json, separators=(",", ":")),
        "tap_step": step_value if step_value is not None else 0,
        "tap_count": len(tap_changers),
    }


async def upload_tap_changer_study(
    eas_client: EasClient,
    pts: List[PowerTransformer],
    transformer_to_tap: Dict[str, Dict[str, Any]],
    name: str,
    description: str,
    tags: List[str],
    styles: List,
) -> None:

    class_to_properties = {
        PowerTransformer: {
            "tap_label": _tap_label_from(transformer_to_tap),
            "tap_json": _tap_json_from(transformer_to_tap),
            "tap_step": _tap_step_from(transformer_to_tap),
            "tap_count": _tap_count_from(transformer_to_tap),
            "type": lambda x: "pt",
        },
    }
    feature_collection = to_geojson_feature_collection(pts, class_to_properties)
    if not feature_collection.features:
        print("No transformer features to display (missing locations or tap changers). Study upload skipped.")
        return
    response = await eas_client.mutation(Mutation.add_studies(studies=[
        StudyInput(
            name=name,
            description=description,
            tags=tags,
            results=[
                StudyResultInput(
                    name=name,
                    sections=[],
                    geo_json_overlay=GeoJsonOverlayInput(
                        data=feature_collection,
                        styles=[s['id'] for s in styles]
                    )
                )
            ],
            styles=styles
        )
        ]
    )
    )
    print(f"Study response: {response}")


def _tap_label_from(pt_to_tap: Dict[str, Dict[str, Any]]):
    def fun(pt: PowerTransformer):
        info = pt_to_tap.get(pt.mrid, {})
        return info.get("tap_label", "n/a")

    return fun


def _tap_json_from(pt_to_tap: Dict[str, Dict[str, Any]]):
    def fun(pt: PowerTransformer):
        info = pt_to_tap.get(pt.mrid, {})
        return info.get("tap_json", "{}")

    return fun


def _tap_step_from(pt_to_tap: Dict[str, Dict[str, Any]]):
    def fun(pt: PowerTransformer):
        info = pt_to_tap.get(pt.mrid, {})
        return info.get("tap_step", 0)

    return fun


def _tap_count_from(pt_to_tap: Dict[str, Dict[str, Any]]):
    def fun(pt: PowerTransformer):
        info = pt_to_tap.get(pt.mrid, {})
        return info.get("tap_count", 0)

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
