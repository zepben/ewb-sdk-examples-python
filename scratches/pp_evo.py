#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import logging
import pandapower as pp

from pp_creators.basic_creator import BasicPandaPowerNetworkCreator
from zepben.evolve import set_direction, NetworkService, Terminal, EnergySource, Feeder

from zepben.evolve import connect_insecure, NetworkConsumerClient

from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS, INCLUDE_ENERGIZED_FEEDERS, INCLUDE_ENERGIZING_SUBSTATIONS, \
    INCLUDE_ENERGIZING_FEEDERS

logger = logging.getLogger(__name__)


async def main():
    async with connect_insecure(
        "localhost",
        50051
    ) as secure_channel:
        print("Creating connection")
        client = NetworkConsumerClient(secure_channel)
        print("Loading Network... ")
        mrid = "_FYSHWK_8HB_ABATTOIR" # This is the feeder that works
        # await client.retrieve_network()
        await client.get_equipment_container(mrid, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)
        print("Done!")
        network = client.service

        feeders = list(network.objects(Feeder))
        print(len(feeders))
        feeder = feeders[0]
        terminal = list(feeder.normal_head_terminal.other_terminals())[0]

        add_energy_source(network, terminal)
        await set_direction().run(network)
        bbn_creator = BasicPandaPowerNetworkCreator(
            logger=logger,
            ec_load_provider=lambda ec: (5000, 0)  # Model each energy consumer with a 5kW nonreactive load
        )
        result = await bbn_creator.create(network)

        print(f"Translation successful: {result.was_successful}")
        print(result.network)
        print()

        print("bus table:")
        print(result.network["bus"])
        print()

        print("load table:")
        print(result.network["load"])
        print()

        print("ext_grid table:")
        print(result.network["ext_grid"])
        print()

        print("line table:")
        print(result.network["line"])
        print()

        print("trafo table:")
        print(result.network["trafo"])
        print()

        print("line_geodata table:")
        print(result.network["line_geodata"])
        print()

        print("Running load flow study...", end="")
        pp.runpp(result.network)
        print("done.")
        print()

        print(result.network)
        print()

        print("res_bus table:")
        print(result.network["res_bus"])
        print()

        print("res_line table:")
        print(result.network["res_line"])
        print()

        print("res_trafo table:")
        print(result.network["res_trafo"])
        print()

        print("res_ext_grid table:")
        print(result.network["res_ext_grid"])
        print()

        print("res_load table:")
        print(result.network["res_load"])
        print()


def add_energy_source(network: NetworkService, connect_to_terminal: Terminal):
    bv = connect_to_terminal.conducting_equipment.base_voltage
    es_t = Terminal(phases=connect_to_terminal.phases)
    es = EnergySource(terminals=[es_t], base_voltage=bv)
    network.add(es_t)
    network.add(es)
    network.connect_terminals(es_t, connect_to_terminal)


if __name__ == "__main__":
    asyncio.run(main())


