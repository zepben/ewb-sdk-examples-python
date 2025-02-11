#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from geojson import Feature, LineString, FeatureCollection, Point
from zepben.eas import Study, Result, GeoJsonOverlay, EasClient
from zepben.evolve import AcLineSegment, EnergyConsumer, connect_with_token, NetworkConsumerClient
# A study is a geographical visualisation of data that is drawn on top of the network.
# This data is typically the result of a load flow simulation.
# Each study may contain multiple results: different visualisations that the user may switch between.
# For example, the first result may display per-unit voltage data, while the second result highlights overloaded equipment.
# Two results are created in this example study: one makes a heatmap of energy consumers and the other highlights LV lines and displays their length.
# Both Evolve App Server and Energy Workbench must be running for this example.
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


with open("../config.json") as f:
    c = json.loads(f.read())


async def main():
    # Fetch network model from Energy Workbench's gRPC service (see ../connecting_to_grpc_service.py for examples on different connection functions)
    print("Connecting to EWB..")
    grpc_channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])
    feeder_mrid = "WD24"
    grpc_client = NetworkConsumerClient(grpc_channel)
    print("Connection established..")
    await grpc_client.get_equipment_container(feeder_mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)
    network = grpc_client.service

    print("Creating study..")
    # Make result that displays a heatmap of energy consumers.
    ec_geojson = []
    for ec in network.objects(EnergyConsumer):
        if ec.location is not None:
            coord = list(ec.location.points)[0]
            ec_feature = Feature(
                id=ec.mrid,
                geometry=Point((coord.x_position, coord.y_position))
            )
            ec_geojson.append(ec_feature)

    ec_result = Result(
        name="Energy Consumers",
        geo_json_overlay=GeoJsonOverlay(
            data=FeatureCollection(ec_geojson),
            styles=["ec-heatmap"]  # Select which Mapbox layers to show for this result
        )
    )

    # Make result that highlights LV lines. Each result is a named GeoJSON overlay.
    lv_lines_geojson = []
    for line in network.objects(AcLineSegment):
        if line.base_voltage_value <= 1000 and line.location is not None:
            line_feature = Feature(
                id=line.mrid,
                geometry=LineString([(p.x_position, p.y_position) for p in line.location.points]),
                properties={
                    "length": line.length  # Numeric and textual data may be added here. It will be displayed and formatted according to the style(s) used.
                }
            )
            lv_lines_geojson.append(line_feature)

    lv_lines_result = Result(
        name="LV Lines",
        geo_json_overlay=GeoJsonOverlay(
            data=FeatureCollection(lv_lines_geojson),
            styles=["lv-lines", "lv-lengths"]  # Select which Mapbox layers to show for this result
        )
    )

    # Create and upload the study.
    study = Study(
        name="Example Study",
        description="Example study with two results.",
        tags=["example"],  # Tags make it easy to search for studies in a large list of them.
        results=[ec_result, lv_lines_result],
        styles=json.load(open("style.json", "r"))  # This is the "layers" property of a Mapbox GL JS style.
        # Layers specify how features are rendered. For more information about layers, read https://docs.mapbox.com/mapbox-gl-js/style-spec/layers/.
        # Each layer may have an entry in the legend via the metadata["zb:legend"] field.
    )
    print("Study created..")
    print("Connecting to EAS..")
    eas_client = EasClient(host=c["host"], port=c["rpc_port"], protocol="https", access_token=c["access_token"])

    print("Connection established..")

    print("Uploading study...")
    response = await eas_client.async_upload_study(study)
    print(response)
    print("Study uploaded! Please check the Evolve Web App.")

    await eas_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
