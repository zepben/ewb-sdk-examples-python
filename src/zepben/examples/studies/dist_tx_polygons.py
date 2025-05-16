#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gpd

from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers

from zepben.evolve import connect_with_token, NetworkConsumerClient, Switch, Site, LvFeeder
from zepben.eas.client.eas_client import EasClient
from zepben.eas.client.study import Study, Result, GeoJsonOverlay

with open("../config.json") as f:
    config = json.load(f)


async def connect():
    channel = connect_with_token(host=config["host"], rpc_port=config["rpc_port"], access_token=config["access_token"], ca_filename=config["ca_path"])

    feeder = "RW1292"
    print(f"Processing feeder {feeder}")
    geojson_features = []
    await process_feeder(feeder, channel, geojson_features)

    print("Uploading study")
    await upload_study({"type": "FeatureCollection", "features": geojson_features})


async def process_feeder(feeder_mrid: str, channel, geojson_features: list):
    print(f"Fetching {feeder_mrid}")
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service

    # Fetches the feeder plus all the LV feeders (dist txs and LV circuits)
    (await network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()

    counter = 0
    for lvf in network_service.objects(LvFeeder):
        print(f"Processing {lvf.name}...")
        points = []

        # Get all the coordinates from the LvFeeder
        for psr in lvf.equipment:
            if psr.location is not None:
                for pp in psr.location.points:
                    points.append((pp.x_position, pp.y_position))

        # Only care about feeders that had more than 3 points - this just excludes anything empty
        # and stops the algorithm from failing
        if len(points) > 3:
            # Build a concave hull of the points
            p = Polygon(points)
            df = pd.DataFrame({'hull': [1]})
            df['geometry'] = p

            gdf = gpd.GeoDataFrame(df, crs='EPSG:4326', geometry='geometry')
            geojson = json.loads(gdf.concave_hull(0.30).to_json())
            feature = geojson["features"][0]
            feature["properties"]["pen"] = counter % 14
            # Add this to the list of features to upload in the study - there should be one feature per zone substation
            geojson_features.append(feature)
            counter += 1


async def upload_study(geojson):
    protocol = config.get("eas_protocol", "https")
    eas_client = EasClient(
        host=config["eas_host"],
        port=config["eas_port"],
        protocol=config.get("eas_protocol", "https"),
        access_token=config["access_token"] if protocol == "https" else None,
        ca_filename=config["ca_path"],
        verify_certificate=False
    )

    styles = [
        {
            "id": "dist tx boundaries",
            "name": "Distribution Transformer Boundaries",
            "type": "line",
            "paint": {
                "line-color": "rgb(0,0,0)",
                "line-width": 3
            },
            "maxzoom": 24,
        },
        {
            "id": "feedersfill",
            "name": "boundaryfill",
            "type": "fill",
            "paint": {
            'fill-color': [
                "match",
                ["get", "pen"],
                0,"#3388FF",
                1,"#8800FF",
                2,"#AAAA00",
                3,"#00AA00",
                4,"#FF00AA",
                5,"#0000AA",
                6,"#AAAAAA",
                7,"#AA0000",
                8,"#00AAFF",
                9,"#AA00AA",
                10,"#CC8800",
                11,"#00AAAA",
                12,"#0000FF",
                13,"#666666",
                14,"#e30707",
                "#cccccc"
            ],
            'fill-opacity': 0.5
            },
            "maxzoom": 24,
        }

    ]

    result = await eas_client.async_upload_study(
        Study(
            name="Dist TX polygons",
            description="Distribution Transformer polygons",
            tags=["tx polygons distribution"],
            results=[
                Result(
                    name="Dist TX Boundaries",
                    geo_json_overlay=GeoJsonOverlay(
                        data=geojson,
                        styles=['feeders', 'feedersfill']
                    )
                )
            ],
            styles=styles
        )
    )
    print(f"EAS upload result: {result}")
    await eas_client.aclose()


if __name__ == "__main__":
    asyncio.run(connect())
