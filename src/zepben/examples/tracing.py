#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

# A Traversal is used to iterate over graph-like structures.
# The Evolve SDK contains several factory functions for traversals that cover common use cases.
import asyncio

from zepben.evolve import Switch, connected_equipment_trace, ConductingEquipmentStep, ConductingEquipment, connected_equipment_breadth_trace, \
    normal_connected_equipment_trace, current_connected_equipment_trace, connectivity_trace, ConnectivityResult, connected_equipment

# For the purposes of this example, we will use the IEEE 13 node feeder.
from zepben.examples.ieee_13_node_test_feeder import network


async def equipment_traces():
    # Equipment traces iterate over equipment connected in a network.
    print("===EQUIPMENT TRACING===")
    print()

    start_item = ConductingEquipmentStep(network.get("vr_650_632", ConductingEquipment))
    visited = set()

    async def print_step(ces: ConductingEquipmentStep, _):
        visited.add(ces.conducting_equipment)
        print(f"\tDepth {ces.step:02d}: {ces.conducting_equipment}")

    # The connected equipment trace iterates through all connected equipment depth-first, and even through open switches.
    # Equipment will be revisited if a shorter path from the starting equipment is found.
    print("Connected Equipment Trace:")
    await connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # There is also a breadth-first version, which guarantees that each equipment is visited at most once.
    print("Connected Equipment Breadth Trace:")
    await connected_equipment_breadth_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment normally connected to the starting equipment.
    # By setting the switch from node 671 to 692 to normally open, the traversal will not trace through the switch.
    network.get("sw_671_692", Switch).set_normally_open(True)
    print("Normal Connected Equipment Trace:")
    await normal_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment normally connected to the starting equipment.
    # By setting the switch from node 671 to 692 to currently open, the traversal will not trace through the switch.
    network.get("sw_671_692", Switch).set_open(True)
    print("Normal Connected Equipment Trace:")
    await current_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()

    # The normal connected equipment trace iterates through all equipment normally connected to the starting equipment.
    # By setting the switch from node 671 to 692 to currently open, the traversal will not trace through the switch.
    network.get("sw_671_692", Switch).set_open(True)
    print("Normal Connected Equipment Trace:")
    await current_connected_equipment_trace().add_step_action(print_step).run(start_item)
    print(f"Number of equipment visited: {len(visited)}")
    print()
    visited.clear()


async def connectivity_traces():
    # Connectivity traces iterate over the connectivity of equipment terminals, rather than the equipment themselves.
    start_item = connected_equipment(network.get("vr_650_632", ConductingEquipment))[0]

    async def print_connectivity(cr: ConnectivityResult, _):
        print(f"{cr.from_terminal.mrid:-<20}{cr.to_terminal.mrid:->20}")

    print("Connectivity trace:")
    await connectivity_trace().add_step_action(print_connectivity).run(start_item)


async def main():
    await equipment_traces()
    await connectivity_traces()


if __name__ == "__main__":
    asyncio.run(main())
