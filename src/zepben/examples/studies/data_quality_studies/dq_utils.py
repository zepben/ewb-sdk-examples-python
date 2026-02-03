#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Type, Union
import json

from geojson import Feature, FeatureCollection
from geojson.geometry import Geometry, LineString, Point
from zepben.ewb import Location, PowerSystemResource


CONFIG_PATH = Path(__file__).resolve().parents[2] / "config.json"


def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r") as f:
        return json.loads(f.read())


def get_zone_mrids(argv: List[str], default: Optional[List[str]] = None) -> List[str]:
    if len(argv) > 1:
        zones = [z.strip() for z in argv[1].split(",") if z.strip()]
        if zones:
            return zones
    return default or []


def chunk(it: Iterable[Any], size: int):
    it = iter(it)
    return iter(lambda: tuple(_take(it, size)), ())


def _take(it: Iterable[Any], size: int):
    items = []
    for _ in range(size):
        try:
            items.append(next(it))
        except StopIteration:
            break
    return items


def line_length_m(line) -> float:
    return float(getattr(line, "length", 0.0) or 0.0)


def terminal_phase_code(terminal) -> Optional[Any]:
    phases = getattr(terminal, "normal_phases", None) or getattr(terminal, "phases", None)
    if phases is None:
        return None
    if hasattr(phases, "as_phase_code"):
        return phases.as_phase_code()
    return phases


def to_geojson_feature_collection(
    psrs: List[PowerSystemResource],
    class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]],
) -> FeatureCollection:
    features = []
    for psr in psrs:
        properties_map = class_to_properties.get(type(psr))
        if properties_map is None:
            continue
        feature = to_geojson_feature(psr, properties_map)
        if feature is not None:
            features.append(feature)
    return FeatureCollection(features)


def to_geojson_feature(
    psr: PowerSystemResource,
    property_map: Dict[str, Callable[[PowerSystemResource], Any]],
) -> Union[Feature, None]:
    geometry = to_geojson_geometry(getattr(psr, "location", None))
    if geometry is None:
        return None
    properties = {k: f(psr) for (k, f) in property_map.items()}
    return Feature(psr.mrid, geometry, properties)


def to_geojson_geometry(location: Location) -> Union[Geometry, None]:
    points = list(location.points) if location is not None else []
    if len(points) > 1:
        return LineString([(point.x_position, point.y_position) for point in points])
    if len(points) == 1:
        return Point((points[0].x_position, points[0].y_position))
    return None


def feature_from_location(
    feature_id: str,
    location: Location,
    properties: Dict[str, Any],
) -> Optional[Feature]:
    geometry = to_geojson_geometry(location)
    if geometry is None:
        return None
    return Feature(feature_id, geometry, properties)


def point_feature(
    feature_id: str,
    x: float,
    y: float,
    properties: Dict[str, Any],
) -> Feature:
    return Feature(feature_id, Point((x, y)), properties)


def no_anomaly_feature_collection(message: str = "No anomalies detected") -> FeatureCollection:
    return FeatureCollection(
        [Feature("no-anomalies", Point((0.0, 0.0)), {"message": message, "type": "no_anomalies"})]
    )
