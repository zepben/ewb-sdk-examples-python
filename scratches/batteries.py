import argparse
import asyncio
import csv
import json
from dataclasses import dataclass
from typing import Optional, Dict

from geojson import Feature, FeatureCollection, Point
from zepben.eas import EasClient, GeoJsonOverlay, Result, Study
from zepben.evolve import EnergyConsumer, NetworkConsumerClient, connect_insecure, connect_with_password
from zepben.protobuf.nc.nc_requests_pb2 import IncludedEnergizedContainers


def read_json_config(config_file_path: str) -> Dict:
    file = open(config_file_path)
    config_dict = json.load(file)
    file.close()
    return config_dict


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--assets",
                        help="File with assets to parse and present",
                        type=str, default="batteries.csv")
    parser.add_argument("--authconfig",
                        help="Auth config file",
                        type=str, default="auth_config.json")
    args = parser.parse_args()
    if args.assets is None:
        raise RuntimeError(
            "Assets file must be provided with --assets parameter")
    return (args.assets, args.authconfig)


@dataclass
class Battery:
    nmi: str
    capacity: float
    power: float
    location: Optional[Point] = None


async def upload_basic_study(assets: str, auth: str):
    # EWB stuff first
    ewb_config = read_json_config(auth)["ewb_server"]

    # for testing with Azure tokens
    # channel = connect_with_secret(
    #     host="ewb.local",
    #     rpc_port="9000",
    #     conf_address="https://ewb.local:8080/ewb/auth",
    #     verify_conf="cachain.crt",
    #     ca_filename="cachain.crt"
    # )
    channel = connect_insecure(
        host=ewb_config["host"],
        rpc_port=ewb_config["rpc_port"],
    )

    eas_config = read_json_config(auth)["eas_server"]
    eas_client = EasClient(
        host=eas_config["host"],
        port=eas_config["port"],
        protocol=eas_config["protocol"],
        # client_id=eas_config["client_id"],
        # username=eas_config.get("username"),
        # password=eas_config.get("password"),
        # client_secret=eas_config.get("client_secret"),
        verify_certificate=eas_config.get("verify_certificate", True),
        ca_filename=eas_config.get("ca_filename")
    )

    batteries = []
    with open(assets, "r") as f:
        csvreader = csv.reader(f)

        # skip the header
        next(csvreader)
        for row in csvreader:
            batteries.append(Battery(
                nmi=f'''_{row[0]}''',
                capacity=float(row[1]),
                power=float(row[2]),
            ))

    markers = []

    ewb_client = NetworkConsumerClient(channel)
    hierarchy = (await ewb_client.get_network_hierarchy()).throw_on_error().value
    for feeder in hierarchy.feeders.values():
        ewb_client2 = NetworkConsumerClient(channel)
        (await ewb_client2.get_equipment_container(
            mrid=feeder.mrid,
            include_energized_containers=IncludedEnergizedContainers.INCLUDE_ENERGIZED_LV_FEEDERS)).throw_on_error()
        ns = ewb_client2.service
        for b in batteries:
            try:
                up = ns.get(b.nmi)
                for eq in up.equipment:
                    if isinstance(eq, EnergyConsumer) and eq.location:
                        p = eq.location.get_point(0)
                        if p:
                            markers.append(Feature(
                                id=b.nmi,
                                geometry=Point((p.longitude, p.latitude)),
                                properties={'label': f'''{b.capacity} KWh'''}
                            ))

                        # Stop checking equipment
                        break
            except KeyError:
                # specifically catch the 'doesn't exist' error
                pass

    styles = json.load(open("style.json", "r"))
    await eas_client.async_upload_study(Study(
        name="Evergen Batteries 2",
        description="Evergen Batteries locations",
        tags=['evergen'],
        results=[
            Result(
                name="Evergen Batteries",
                geo_json_overlay=GeoJsonOverlay(
                    data=FeatureCollection(markers),
                    styles=['batteries', 'labels']
                )
            )
        ],
        styles=styles
    ))
    await eas_client.aclose()


if __name__ == '__main__':
    # Open the CSV and parse the data first
    (assets, auth) = parse_arguments()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(upload_basic_study(assets, auth))
