#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json

from geojson import Feature, LineString, FeatureCollection, Point
from zepben.eas import Study, Result, GeoJsonOverlay, EasClient
from zepben.evolve import connect_insecure, SyncNetworkConsumerClient, AcLineSegment, EnergyConsumer


# A study is a geographical visualisation of data that is drawn on top of the network.
# This data is typically the result of a load flow simulation.
# Each study may contain multiple results: different visualisations that the user may switch between.
# For example, the first result may display per-unit voltage data, while the second result highlights overloaded equipment.
# Two results are created in this example study: one makes a heatmap of energy consumers and the other highlights LV lines and displays their length.
# Both Evolve App Server and Energy Workbench must be running for this example.

def main():
    # Fetch network model from Energy Workbench's gRPC service (see ../connecting_to_grpc_service.py for examples on different connection functions)
    grpc_channel = connect_insecure("localhost", 9001)
    grpc_client = SyncNetworkConsumerClient(grpc_channel)
    grpc_client.retrieve_network()  # Use get_feeder("<feeder-id>") instead to fetch only a specific feeder
    network = grpc_client.service

    # Make result that displays a heatmap of energy consumers.
    ec_geojson = []
    for ec in network.objects(EnergyConsumer):
        if ec.location is not None:
            x, y = list(ec.location.points)[0]
            ec_feature = Feature(
                id=ec.mrid,
                geometry=Point((x, y))
            )
            ec_geojson.append(ec_feature)

    ec_result = Result(
        name="Energy Consumer Active Load",
        geo_json_overlay=GeoJsonOverlay(
            data=FeatureCollection(ec_geojson),
            styles=["ec-heatmap"]
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
            styles=["lv-lines", "lv-lengths"]  # Select which styles to apply to this result
        )
    )

    # Create and upload the study.
    study = Study(
        name="Example Study",
        description="Example study with two results.",
        tags=["example"],  # Tags make it easy to search for studies in a large list of them.
        results=[lv_lines_result, ec_result],
        styles=json.load(open("style.json", "r"))  # See Mapbox style specification documentation for information on making a style JSON.
    )
    eas_client = EasClient(
        # Replace these values with the host/port and credentials for the instance of EAS you would like to upload the study to.
        host="localhost",
        port=7654,
        client_id="<client-id>",
        username="<username or email>",
        password="<password>"
    )

    print("Uploading study...")
    eas_client.upload_study(study)
    print("Study uploaded! Please check the Evolve Web App.")

    eas_client.close()


if __name__ == "__main__":
    main()
