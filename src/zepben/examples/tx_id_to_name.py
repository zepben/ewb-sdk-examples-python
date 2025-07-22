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
import os.path
from dataclasses import dataclass
import pandas as pd

from zepben.evolve import NetworkConsumerClient, connect_with_token, PowerTransformer

OUTPUT_FILE = "transformer_id_mapping.csv"
HEADER = True

with open("./config.json") as f:
    c = json.loads(f.read())


async def connect():
    channel = connect_with_token(host=c["host"], rpc_port=c["rpc_port"], access_token=c["access_token"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)

    if os.path.exists(OUTPUT_FILE):
        print(f"Output file {OUTPUT_FILE} already exists, please delete it if you would like to regenerate.")
        return

    network_hierarchy = (await network_client.get_network_hierarchy()).throw_on_error().value

    print("Network hierarchy:")
    for gr in network_hierarchy.geographical_regions.values():
        print(f"- Geographical region: {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - Subgeographical region: {sgr.name}")
            for sub in sgr.substations:
                print(f"    - Zone Substation: {sub.name}")
                await process_nodes(sub.mrid, channel)
                for fdr in sub.feeders:
                    print(f"      - Processing Feeder: {fdr.name}")
                    await process_nodes(fdr.mrid, channel)
                return  # Only process the first zone...


@dataclass
class NetworkObject(object):
    dist_tx_id: str
    dist_tx_name: str
    container: str
    container_mrid: str


async def process_nodes(container_mrid: str, channel):
    global HEADER
    print("Fetching from server ...")
    network_client = NetworkConsumerClient(channel=channel)
    network_service = network_client.service
    (await network_client.get_equipment_container(container_mrid)).throw_on_error()
    container = network_service.get(container_mrid)
    container_name = container.name

    print("Processing equipment ...")
    network_objects = []
    for equip in network_service.objects(PowerTransformer):
        no = NetworkObject(equip.mrid, equip.name, container_name, container_mrid)
        network_objects.append(no)

    network_objects = pd.DataFrame(network_objects)
    network_objects.to_csv(OUTPUT_FILE, index=False, mode='a', header=HEADER)
    print(f"Finished processing {container_mrid}")
    if HEADER:
        HEADER = False


if __name__ == "__main__":
    asyncio.run(connect())
