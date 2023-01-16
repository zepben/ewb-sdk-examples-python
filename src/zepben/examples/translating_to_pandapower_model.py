#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import logging

from pp_creators.basic_creator import BasicPandaPowerNetworkCreator
from zepben.evolve import set_direction, NetworkService, Terminal, EnergySource

from zepben.examples.ieee_13_node_test_feeder import network

logger = logging.getLogger(__name__)


async def main():
    add_energy_source(network, network["br_650_t1"])
    await set_direction().run(network)
    result = await BasicPandaPowerNetworkCreator(logger=logger).create(network)
    print(f"Translation successful: {result.was_successful}")
    print(result.network)
    print()

    print("bus table:")
    print(result.network["bus"])
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


def add_energy_source(network: NetworkService, connect_to_terminal: Terminal):
    bv = connect_to_terminal.conducting_equipment.base_voltage
    es_t = Terminal(phases=connect_to_terminal.phases)
    es = EnergySource(terminals=[es_t], base_voltage=bv)
    network.add(es_t)
    network.add(es)
    network.connect_terminals(es_t, connect_to_terminal)


if __name__ == "__main__":
    asyncio.run(main())
