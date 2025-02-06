#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.evolve import connect_with_token, NetworkConsumerClient

with open("config.json") as f:
    c = json.loads(f.read())


async def main():
    # See connecting_to_grpc_service.py for examples of each connect function
    print("Connecting to EWB..")
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"])
    client = NetworkConsumerClient(channel)
    print("Connection established..")
    # Fetch network hierarchy
    network_hierarchy = await client.get_network_hierarchy()

    print("Network hierarchy:")
    for gr in network_hierarchy.result.geographical_regions.values():
        print(f"- {gr.name}")
        for sgr in gr.sub_geographical_regions:
            print(f"  - {sgr.name}")
            for sub in sgr.substations:
                print(f"    - {sub.name}")
                for fdr in sub.feeders:
                    print(f"      - {fdr.name}")


if __name__ == "__main__":
    asyncio.run(main())
