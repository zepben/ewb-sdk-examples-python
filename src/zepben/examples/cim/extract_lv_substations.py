#  Copyright 2026 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json

from zepben.ewb import connect_with_token, NetworkConsumerClient, HvCustomer, IncludedEnergizedContainers, LvSubstation

from zepben.examples import CONFIG_DIR

with open(f"{CONFIG_DIR}/config.json") as f:
    c = json.loads(f.read())


async def extract_lv_substations_for_feeder(feeder_mrid: str):
    channel = connect_with_token(host=c["host"], access_token=c["access_token"], rpc_port=c["rpc_port"], ca_filename=c["ca_path"])
    network_client = NetworkConsumerClient(channel=channel)
    network = network_client.service

    # Fetch the feeder from the server
    # Note LV feeders are not required to find LvSubstations, but we want to print the downstream LvFeeders for the LV Substation.
    (await network_client.get_equipment_container(feeder_mrid, include_energized_containers=IncludedEnergizedContainers.LV_FEEDERS)).throw_on_error()

    # Print each LV Substation and the number of equipment inside it
    for lv_substation in network.objects(LvSubstation):
        print(f"{lv_substation.name} - num equipment: {lv_substation.num_equipment()}")
        # Print all LvFeeders in the LvSubstation, plus the number of equipment inside each LvFeeder.
        for lvf in lv_substation.normal_energized_lv_feeders:
            print(f"  {lvf} - num equipment: {lvf.num_equipment()}")


if __name__ == "__main__":
    asyncio.run(extract_lv_substations_for_feeder("YVE-014"))
