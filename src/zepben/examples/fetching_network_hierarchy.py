#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

from zepben.evolve import SyncNetworkConsumerClient, connect_insecure


def main():
    # See connecting_to_grpc_service.py for examples of each connect function
    channel = connect_insecure(host="EWB hostname", rpc_port=1234)
    client = SyncNetworkConsumerClient(channel=channel)
    # Fetch network hierarchy
    network_hierarchy = client.get_network_hierarchy()

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
    main()
