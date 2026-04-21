#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.ewb import connect_with_token, NetworkConsumerClient, HvCustomer

from zepben.examples import CONFIG_DIR

with open(f"{CONFIG_DIR}/config.json") as f:
    c = json.loads(f.read())


async def extract_hv_customers_for_feeder(feeder_mrid: str):
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)
    network = network_client.service

    # Fetch the feeder from the server - Note LV feeders are not required to find HV customers.
    (await network_client.get_equipment_container(feeder_mrid)).throw_on_error()

    # Print each HV customer and all its usage points
    for hv_customer in network.objects(HvCustomer):
        print(f"{hv_customer.name} - num equipment: {hv_customer.num_equipment()}")
        # Print any UsagePoints
        for equip in hv_customer.equipment:
            if equip.usage_points:
                for up in equip.usage_points:
                    print(f"  {equip} - {up} - {' | '.join([f'{name.type.name} {name.name}' for name in up.names])}")


if __name__ == "__main__":
    asyncio.run(extract_hv_customers_for_feeder("YVE-014"))