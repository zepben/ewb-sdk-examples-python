#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import argparse
from dataclasses import dataclass
import asyncio
import calendar
import json
import os
import sys
import threading
from datetime import date, datetime
from itertools import islice
from typing import List, Dict, Tuple, Callable, Any, Union, Type, Optional

import requests
from geojson import FeatureCollection, Feature
from geojson.geometry import Geometry, LineString, Point
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import Study, Result, GeoJsonOverlay
from zepben.ewb import (
    PowerTransformer,
    NetworkConsumerClient,
    Feeder,
    PowerSystemResource,
    Location,
    connect_with_token,
    IncludedEnergizedContainers,
)


DEFAULT_CONFIG_PATH = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "config.json"))


def chunk(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def _build_base_url(host: str, port: Optional[int]) -> str:
    host = host.rstrip("/")
    if host.startswith("http://") or host.startswith("https://"):
        base = host
    else:
        base = f"https://{host}"

    if port and port not in (80, 443):
        # Only append if a port is not already present
        if ":" not in base.split("//", 1)[-1]:
            base = f"{base}:{port}"
    return base


def _subtract_months(d: date, months: int) -> date:
    year = d.year - (months // 12)
    month = d.month - (months % 12)
    if month <= 0:
        year -= 1
        month += 12
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


class LoadApiClient:
    def __init__(
        self,
        base_url: str,
        access_token: str,
        system_tag: str = "EWB",
        timeout_seconds: int = 10,
        verify: Union[bool, str] = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.system_tag = system_tag
        self.timeout_seconds = timeout_seconds
        self.verify = verify
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            }
        )

    def close(self):
        self.session.close()

    def get_max_demand(self, asset_id: str, from_date: str, to_date: str) -> Dict[str, Any]:
        return self._get_profile(f"/ewb/energy/profiles/api/v1/max-demand/{asset_id}", from_date, to_date)

    def get_min_demand(self, asset_id: str, from_date: str, to_date: str) -> Dict[str, Any]:
        return self._get_profile(f"/ewb/energy/profiles/api/v1/min-demand/{asset_id}", from_date, to_date)

    def _get_profile(self, path: str, from_date: str, to_date: str) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        params = {
            "fromDate": from_date,
            "toDate": to_date,
            "systemTag": self.system_tag,
        }
        response = self.session.get(url, params=params, timeout=self.timeout_seconds, verify=self.verify)
        response.raise_for_status()
        return response.json()


class LoadApiConfig:
    def __init__(
        self,
        base_url: str,
        access_token: str,
        system_tag: str,
        timeout_seconds: int,
        verify: Union[bool, str],
    ):
        self.base_url = base_url
        self.access_token = access_token
        self.system_tag = system_tag
        self.timeout_seconds = timeout_seconds
        self.verify = verify


_thread_local = threading.local()


def _get_thread_client(config: LoadApiConfig) -> LoadApiClient:
    client = getattr(_thread_local, "client", None)
    if client is None:
        client = LoadApiClient(
            base_url=config.base_url,
            access_token=config.access_token,
            system_tag=config.system_tag,
            timeout_seconds=config.timeout_seconds,
            verify=config.verify,
        )
        _thread_local.client = client
    return client


def _iter_series_entries(series: Any):
    if isinstance(series, dict):
        yield series
        return
    if not isinstance(series, list):
        return
    for entry in series:
        if isinstance(entry, dict):
            yield entry
            continue
        if isinstance(entry, list):
            for nested in entry:
                if isinstance(nested, dict):
                    yield nested


def _first_dict(value: Any) -> Optional[Dict[str, Any]]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list) and value:
        for item in value:
            if isinstance(item, dict):
                return item
    return None


def _preview_payload(payload: Any, max_chars: int = 1200) -> str:
    try:
        text = json.dumps(payload, default=str)
    except (TypeError, ValueError):
        text = str(payload)
    if len(text) > max_chars:
        return f"{text[:max_chars]}... (truncated {len(text) - max_chars} chars)"
    return text


@dataclass
class DebugConfig:
    log_summary: bool = False
    log_samples: int = 0
    log_api_samples: int = 0


class DebugState:
    def __init__(self, config: DebugConfig):
        self.config = config
        self._samples_left = max(config.log_samples, 0)
        self._api_left = max(config.log_api_samples, 0)
        self._lock = threading.Lock()

    def log_api(self, pt: PowerTransformer, max_profile: Any, min_profile: Any):
        with self._lock:
            if self._api_left <= 0:
                return
            self._api_left -= 1
        print(f"[debug] API payloads for {pt.mrid}:")
        print(f"[debug] max_profile={_preview_payload(max_profile)}")
        print(f"[debug] min_profile={_preview_payload(min_profile)}")

    def log_stats(self, pt: PowerTransformer, rating_va: float, max_data: Any, min_data: Any, stats: Dict[str, Any]):
        with self._lock:
            if self._samples_left <= 0:
                return
            self._samples_left -= 1
        sample = {
            "rating_va": rating_va,
            "rating_kva": stats.get("transformer_rating_kva"),
            "has_max_profile": stats.get("has_max_profile"),
            "has_min_profile": stats.get("has_min_profile"),
            "max_import_util_percent": stats.get("max_import_util_percent"),
            "max_export_util_percent": stats.get("max_export_util_percent"),
            "min_export_util_percent": stats.get("min_export_util_percent"),
            "min_import_util_percent": stats.get("min_import_util_percent"),
            "max_demand_date": stats.get("max_demand_date"),
            "min_demand_date": stats.get("min_demand_date"),
            "max_demand_season": stats.get("max_demand_season"),
            "min_demand_season": stats.get("min_demand_season"),
        }
        print(f"[debug] Stats for {pt.mrid}: {sample}")
        print(f"[debug] max_data={max_data}")
        print(f"[debug] min_data={min_data}")


def _extract_maximums(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if payload is None:
        return None
    if isinstance(payload, list):
        payload = _first_dict(payload)
        if payload is None:
            return None
    if not isinstance(payload, dict):
        return None
    if payload.get("error"):
        return None

    results = payload.get("results") or []
    if not results:
        return None

    for result in results:
        series = result.get("series") if isinstance(result, dict) else None
        if not series:
            continue

        for entry in _iter_series_entries(series):
            energy = _first_dict(entry.get("energy")) if isinstance(entry, dict) else None
            if energy is None and isinstance(entry, dict) and ("maximums" in entry or "date" in entry):
                energy = entry
            if not isinstance(energy, dict):
                continue

            maximums = energy.get("maximums") or {}

            return {
                "date": energy.get("date"),
                "kwIn": maximums.get("kwIn"),
                "kwOut": maximums.get("kwOut"),
                "kwNet": maximums.get("kwNet"),
                "kvaNet": maximums.get("kvaNet"),
                "pf": maximums.get("pf"),
            }

    return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _util_percent(value_kw: Optional[float], rating_va: float) -> Optional[float]:
    if value_kw is None or rating_va <= 0:
        return None
    return round((value_kw / rating_va) * 100, 1)


async def main():
    zone_mrids, feeder_mrids, mode, config_path, max_workers, debug_cfg, seasonal_shapes_flag = _parse_args(
        sys.argv[1:]
    )
    with open(config_path) as f:
        c = json.loads(f.read())
    print(f"Start time: {datetime.now()}")

    rpc_channel = connect_with_token(
        host=c["host"],
        access_token=c["access_token"],
        rpc_port=c["rpc_port"],
        ca_filename=c.get("ca_filename"),
        timeout_seconds=c.get("timeout_seconds", 5),
        debug=bool(c.get("debug", False)),
        skip_connection_test=bool(c.get("skip_connection_test", False)),
    )
    if mode == "zones":
        client = NetworkConsumerClient(rpc_channel)
        hierarchy = (await client.get_network_hierarchy()).throw_on_error()
        substations = hierarchy.value.substations

        print(f"Collecting feeders from zones {', '.join(zone_mrids)}.")
        feeder_mrids = []
        for zone_mrid in zone_mrids:
            if zone_mrid in substations:
                for feeder in substations[zone_mrid].feeders:
                    feeder_mrids.append(feeder.mrid)
    else:
        print(f"Running for feeders {', '.join(feeder_mrids)}.")

    print(f"Feeders to be processed: {', '.join(feeder_mrids)}")

    transformers_by_id: Dict[str, PowerTransformer] = {}

    # Process the feeders in batches of 3, using asyncio, for performance
    batches = chunk(feeder_mrids, 3)
    for feeders in batches:
        futures = []
        rpc_channel = connect_with_token(
            host=c["host"],
            access_token=c["access_token"],
            rpc_port=c["rpc_port"],
            ca_filename=c.get("ca_filename"),
            timeout_seconds=c.get("timeout_seconds", 5),
            debug=bool(c.get("debug", False)),
            skip_connection_test=bool(c.get("skip_connection_test", False)),
        )
        print(f"Processing feeders {', '.join(feeders)}")
        for feeder_mrid in feeders:
            futures.append(asyncio.ensure_future(fetch_feeder_transformers(feeder_mrid, rpc_channel)))

        for future in futures:
            transformers = await future
            for pt in transformers:
                transformers_by_id[pt.mrid] = pt

    all_transformers = list(transformers_by_id.values())
    print(f"Collected {len(all_transformers)} transformers")

    today = date.today()
    start_date = _subtract_months(today, 18)
    from_date = start_date.isoformat()
    to_date = today.isoformat()
    print(f"Load API date range: {from_date} to {to_date}")

    base_url = _build_base_url(c["host"], c.get("rpc_port"))
    system_tag = c.get("load_api_system_tag", "EWB")
    timeout_seconds = c.get("timeout_seconds", 10)
    verify = c.get("ca_filename") or True
    season_hemisphere = c.get("season_hemisphere", "southern")
    seasonal_shapes = bool(c.get("seasonal_shapes", False)) or seasonal_shapes_flag

    load_config = LoadApiConfig(
        base_url=base_url,
        access_token=c["access_token"],
        system_tag=system_tag,
        timeout_seconds=timeout_seconds,
        verify=verify,
    )

    debug_state = (
        DebugState(debug_cfg)
        if debug_cfg.log_summary or debug_cfg.log_samples or debug_cfg.log_api_samples
        else None
    )

    transformer_to_stats: Dict[str, Dict[str, Any]] = {}
    semaphore = asyncio.Semaphore(max_workers)
    tasks = [
        asyncio.create_task(
            _fetch_transformer_utilisation_async(
                pt, load_config, from_date, to_date, season_hemisphere, semaphore, debug_state
            )
        )
        for pt in all_transformers
    ]
    completed = 0
    for future in asyncio.as_completed(tasks):
        mrid, stats = await future
        transformer_to_stats[mrid] = stats
        completed += 1
        if completed % 25 == 0 or completed == len(tasks):
            print(f"Load API progress: {completed}/{len(tasks)}")

    print(f"Created Study for {len(all_transformers)} transformers")

    styles = json.load(open("style_transformer_utilisation.json", "r"))
    max_feature_collection = to_geojson_feature_collection(
        all_transformers,
        {
            PowerTransformer: {
                "max_import_util_percent": _stat_getter(transformer_to_stats, "max_import_util_percent"),
                "max_import_label": _stat_getter(transformer_to_stats, "max_import_label"),
                "max_import_util_kw_percent": _stat_getter(transformer_to_stats, "max_import_util_kw_percent"),
                "max_import_kw": _stat_getter(transformer_to_stats, "max_import_kw"),
                "max_import_kva": _stat_getter(transformer_to_stats, "max_import_kva"),
                "max_demand_date": _stat_getter(transformer_to_stats, "max_demand_date"),
                "max_demand_season": _stat_getter(transformer_to_stats, "max_demand_season"),
                "transformer_rating_va": _stat_getter(transformer_to_stats, "transformer_rating_va"),
                "transformer_rating_kva": _stat_getter(transformer_to_stats, "transformer_rating_kva"),
                "has_max_profile": _stat_getter(transformer_to_stats, "has_max_profile"),
                "type": lambda x: "pt",
            }
        },
    )
    min_feature_collection = to_geojson_feature_collection(
        all_transformers,
        {
            PowerTransformer: {
                "min_export_util_percent": _stat_getter(transformer_to_stats, "min_export_util_percent"),
                "min_export_label": _stat_getter(transformer_to_stats, "min_export_label"),
                "min_export_util_kw_percent": _stat_getter(transformer_to_stats, "min_export_util_kw_percent"),
                "min_export_kw": _stat_getter(transformer_to_stats, "min_export_kw"),
                "min_export_kva": _stat_getter(transformer_to_stats, "min_export_kva"),
                "min_demand_date": _stat_getter(transformer_to_stats, "min_demand_date"),
                "min_demand_season": _stat_getter(transformer_to_stats, "min_demand_season"),
                "transformer_rating_va": _stat_getter(transformer_to_stats, "transformer_rating_va"),
                "transformer_rating_kva": _stat_getter(transformer_to_stats, "transformer_rating_kva"),
                "has_min_profile": _stat_getter(transformer_to_stats, "has_min_profile"),
                "type": lambda x: "pt",
            }
        },
    )

    if debug_state and debug_state.config.log_summary:
        missing_location = 0
        for pt in all_transformers:
            points = list(pt.location.points) if pt.location is not None else []
            if not points:
                missing_location += 1

        stats_values = list(transformer_to_stats.values())
        has_max_profile = sum(1 for stats in stats_values if stats.get("has_max_profile"))
        has_min_profile = sum(1 for stats in stats_values if stats.get("has_min_profile"))
        rating_zero = sum(1 for stats in stats_values if stats.get("transformer_rating_va") in (0, 0.0))

        max_import_values = [
            stats.get("max_import_util_percent")
            for stats in stats_values
            if stats.get("has_max_profile") and stats.get("max_import_util_percent") is not None
        ]
        min_export_values = [
            stats.get("min_export_util_percent")
            for stats in stats_values
            if stats.get("has_min_profile") and stats.get("min_export_util_percent") is not None
        ]

        print(
            "[debug] Summary:"
            f" total_transformers={len(all_transformers)}"
            f" max_features={len(max_feature_collection.features)}"
            f" min_features={len(min_feature_collection.features)}"
            f" missing_location={missing_location}"
            f" rating_zero={rating_zero}"
            f" has_max_profile={has_max_profile}"
            f" has_min_profile={has_min_profile}"
        )
        if max_import_values:
            print(
                "[debug] Max import utilisation (percent):"
                f" min={min(max_import_values)}"
                f" max={max(max_import_values)}"
            )
        if min_export_values:
            print(
                "[debug] Min export utilisation (percent):"
                f" min={min(min_export_values)}"
                f" max={max(min_export_values)}"
            )
        sample_feature = None
        if max_feature_collection.features:
            sample_feature = max_feature_collection.features[0]
        elif min_feature_collection.features:
            sample_feature = min_feature_collection.features[0]
        if sample_feature is not None:
            sample_props = getattr(sample_feature, "properties", None)
            if sample_props is None and isinstance(sample_feature, dict):
                sample_props = sample_feature.get("properties")
            if sample_props is not None:
                print(f"[debug] Sample feature properties: {sample_props}")

    results = []
    max_styles = ["max-demand-utilisation", "max-demand-label"]
    min_styles = ["min-demand-utilisation", "min-demand-label"]
    if seasonal_shapes:
        max_styles = ["max-demand-season-shape", "max-demand-label"]
        min_styles = ["min-demand-season-shape", "min-demand-label"]

    if max_feature_collection.features:
        results.append(
            Result(
                name="Max Demand Utilisation (Import)",
                geo_json_overlay=GeoJsonOverlay(
                    data=max_feature_collection,
                    styles=max_styles,
                ),
            )
        )
    if min_feature_collection.features:
        results.append(
            Result(
                name="Min Demand Utilisation (Export)",
                geo_json_overlay=GeoJsonOverlay(
                    data=min_feature_collection,
                    styles=min_styles,
                ),
            )
        )

    if not results:
        print("No transformer features to display (missing locations). Study upload skipped.")
        return

    scope_mrids = zone_mrids if mode == "zones" else feeder_mrids
    scope_label = ", ".join(scope_mrids)
    scope_tag = "-".join(scope_mrids)

    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])
    print(f"Uploading Study for {mode} {scope_label} ...")
    await eas_client.async_upload_study(
        Study(
            name=f"Transformer utilisation (import/export) ({scope_label})",
            description=(
                "Max-demand import utilisation (kwIn) and min-demand export utilisation (kwOut) "
                "for the last 18 months via the Energy Profiles API."
            ),
            tags=["transformer_utilisation", "load_api", scope_tag],
            results=results,
            styles=styles,
        )
    )
    await eas_client.aclose()
    print("Uploaded Study")

    print(f"Finish time: {datetime.now()}")


async def fetch_feeder_transformers(
    feeder_mrid: str,
    rpc_channel,
) -> List[PowerTransformer]:
    print(f"Fetching Feeder {feeder_mrid}")
    client = NetworkConsumerClient(rpc_channel)

    result = (
        await client.get_equipment_container(
            mrid=feeder_mrid,
            expected_class=Feeder,
            include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS,
        )
    )
    if result.was_failure:
        print(f"Failed: {result.thrown}")
        return []

    network = client.service
    print(f"Finished fetching Feeder {feeder_mrid}")

    return [pt for pt in network.objects(PowerTransformer)]


def fetch_transformer_utilisation(
    pt: PowerTransformer,
    load_config: LoadApiConfig,
    from_date: str,
    to_date: str,
    season_hemisphere: str,
    debug_state: Optional[DebugState] = None,
) -> Dict[str, Any]:
    load_client = _get_thread_client(load_config)
    rating_va = _transformer_rating_va(pt)
    rating_kva = rating_va / 1000.0 if rating_va else 0.0

    max_profile = None
    min_profile = None

    try:
        max_profile = load_client.get_max_demand(pt.mrid, from_date, to_date)
    except requests.RequestException as ex:
        print(f"Max-demand request failed for {pt.mrid}: {ex}")

    try:
        min_profile = load_client.get_min_demand(pt.mrid, from_date, to_date)
    except requests.RequestException as ex:
        print(f"Min-demand request failed for {pt.mrid}: {ex}")

    max_data = _extract_maximums(max_profile) if max_profile else None
    min_data = _extract_maximums(min_profile) if min_profile else None

    max_kw_in = _safe_float(max_data.get("kwIn")) if max_data else None
    max_kw_out = _safe_float(max_data.get("kwOut")) if max_data else None
    max_kva_net = _safe_float(max_data.get("kvaNet")) if max_data else None
    min_kw_in = _safe_float(min_data.get("kwIn")) if min_data else None
    min_kw_out = _safe_float(min_data.get("kwOut")) if min_data else None
    min_kva_net = _safe_float(min_data.get("kvaNet")) if min_data else None

    max_import_util_kw = _util_percent(max_kw_in, rating_kva)
    min_export_util_kw = _util_percent(abs(min_kw_out) if min_kw_out is not None else None, rating_kva)
    max_import_util = _util_percent(max_kva_net, rating_kva)
    min_export_util = _util_percent(abs(min_kva_net) if min_kva_net is not None else None, rating_kva)
    max_season = _season_from_date(max_data.get("date") if max_data else None, season_hemisphere)
    min_season = _season_from_date(min_data.get("date") if min_data else None, season_hemisphere)

    stats = {
        "max_import_util_percent": max_import_util or 0,
        "min_export_util_percent": min_export_util or 0,
        "max_import_label": _percent_label(max_import_util),
        "min_export_label": _percent_label(min_export_util),
        "max_import_util_kw_percent": max_import_util_kw or 0,
        "min_export_util_kw_percent": min_export_util_kw or 0,
        "max_import_kw": max_kw_in or 0,
        "min_export_kw": abs(min_kw_out) if min_kw_out is not None else 0,
        "max_import_kva": max_kva_net or 0,
        "min_export_kva": abs(min_kva_net) if min_kva_net is not None else 0,
        "max_demand_date": max_data.get("date") if max_data else None,
        "min_demand_date": min_data.get("date") if min_data else None,
        "max_demand_season": max_season,
        "min_demand_season": min_season,
        "transformer_rating_va": rating_va,
        "transformer_rating_kva": rating_kva,
        "has_max_profile": max_data is not None,
        "has_min_profile": min_data is not None,
        "max_export_util_percent": _util_percent(abs(max_kw_out) if max_kw_out is not None else None, rating_kva) or 0,
        "min_import_util_percent": _util_percent(min_kw_in, rating_kva) or 0,
    }

    if debug_state:
        debug_state.log_api(pt, max_profile, min_profile)
        debug_state.log_stats(pt, rating_va, max_data, min_data, stats)

    return stats


async def _fetch_transformer_utilisation_async(
    pt: PowerTransformer,
    load_config: LoadApiConfig,
    from_date: str,
    to_date: str,
    season_hemisphere: str,
    semaphore: asyncio.Semaphore,
    debug_state: Optional[DebugState] = None,
) -> Tuple[str, Dict[str, Any]]:
    async with semaphore:
        stats = await asyncio.to_thread(
            fetch_transformer_utilisation,
            pt,
            load_config,
            from_date,
            to_date,
            season_hemisphere,
            debug_state,
        )
        return pt.mrid, stats


def _percent_label(value: Optional[float]) -> str:
    return "n/a" if value is None else f"{value}%"


def _season_from_date(value: Optional[str], hemisphere: str = "southern") -> Optional[str]:
    if not value:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None

    month = parsed.month
    hemi = hemisphere.strip().lower() if hemisphere else "southern"
    is_northern = hemi.startswith("n")

    if is_northern:
        if month in (12, 1, 2):
            return "Winter"
        if month in (3, 4, 5):
            return "Spring"
        if month in (6, 7, 8):
            return "Summer"
        return "Autumn"

    if month in (12, 1, 2):
        return "Summer"
    if month in (3, 4, 5):
        return "Autumn"
    if month in (6, 7, 8):
        return "Winter"
    return "Spring"


def _transformer_rating_va(pt: PowerTransformer) -> float:
    ratings: List[float] = []
    for end in pt.ends:
        if end.rated_s is not None:
            ratings.append(end.rated_s)
        else:
            for rating in end.s_ratings:
                if rating and rating.rated_s is not None:
                    ratings.append(rating.rated_s)
    return max(ratings) if ratings else 0


def _stat_getter(stats: Dict[str, Dict[str, Any]], key: str, default: Any = None):
    def fun(pt: PowerTransformer):
        return stats.get(pt.mrid, {}).get(key, default)

    return fun


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


def _parse_args(argv: List[str]) -> Tuple[List[str], List[str], str, str, int, DebugConfig, bool]:
    parser = argparse.ArgumentParser(
        description="Generate a transformer utilisation study for one or more zones or feeders.",
    )
    parser.add_argument(
        "--mode",
        choices=["zones", "feeders"],
        default="zones",
        help="Interpret positional values as zones or feeders (default: zones).",
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
        "--max-workers",
        type=int,
        default=None,
        help="Maximum concurrent load API requests (default: 8).",
    )
    parser.add_argument(
        "--log-summary",
        action="store_true",
        help="Log summary counts (profiles, ratings, locations).",
    )
    parser.add_argument(
        "--log-samples",
        type=int,
        default=0,
        help="Log computed stats for the first N transformers.",
    )
    parser.add_argument(
        "--log-api-samples",
        type=int,
        default=0,
        help="Log raw max/min API payloads for the first N transformers.",
    )
    parser.add_argument(
        "--seasonal-shapes",
        action="store_true",
        help="Use season shapes (Summer/Autumn/Winter/Spring) for icons.",
    )
    parser.add_argument(
        "ids",
        nargs="*",
        help="Zone codes or feeder MRIDs (positional values override --zones/--feeders).",
    )
    args = parser.parse_args(argv)

    def _split_values(values: Any) -> List[str]:
        if isinstance(values, list):
            items = values
        elif isinstance(values, str):
            items = values.split(",")
        else:
            items = []
        return [item.strip() for item in items if item and item.strip()]

    max_workers = args.max_workers or 8
    debug_cfg = DebugConfig(
        log_summary=bool(args.log_summary),
        log_samples=int(args.log_samples or 0),
        log_api_samples=int(args.log_api_samples or 0),
    )

    if args.mode == "feeders":
        feeder_mrids = _split_values(args.ids) or _split_values(args.feeders)
        if not feeder_mrids:
            raise ValueError("At least one feeder MRID is required.")
        return [], feeder_mrids, args.mode, args.config, max_workers, debug_cfg, bool(args.seasonal_shapes)

    zone_mrids = _split_values(args.ids) or _split_values(args.zones)

    if not zone_mrids:
        raise ValueError("At least one zone code is required.")

    return zone_mrids, [], args.mode, args.config, max_workers, debug_cfg, bool(args.seasonal_shapes)


if __name__ == "__main__":
    asyncio.run(main())
