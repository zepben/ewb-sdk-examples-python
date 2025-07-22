#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import (
    EnergySource, AcLineSegment, Fuse, PowerTransformer, Breaker, EnergyConsumer, NetworkService,
    Terminal, connected_equipment, ConductingEquipment, PhaseCode, connected_terminals,
    ConnectivityResult
)

def build_network() -> NetworkService:
    """
    This function will return a network model resembling the below.

       source     consumer
         |           |
        line       line
         |           |
        fuse      breaker
         |           |
    transformer     fuse
         |           |
         +-----------+
    """

    # We create the objects, and their Terminals
    _es = EnergySource(mrid="es", terminals=[
        Terminal(mrid="es-t")
    ])

    _hv_line = AcLineSegment(mrid="hv_line", terminals=[
        Terminal(mrid="hv_line_t1"),
        Terminal(mrid="hv_line_t2")
    ])

    _hv_fuse = Fuse(mrid="hv_fuse", terminals=[
        Terminal(mrid="hv_fuse_t1"),
        Terminal(mrid="hv_fuse_t2")
    ])

    _tx = PowerTransformer(mrid="tx", terminals=[
        Terminal(mrid="tx_t1"),
        Terminal(mrid="tx_t2", phases=PhaseCode.ABCN)
    ])

    _lv_fuse = Fuse(mrid="lv_fuse", terminals=[
        Terminal(mrid="lv_fuse_t1", phases=PhaseCode.ABCN),
        Terminal(mrid="lv_fuse_t2", phases=PhaseCode.ABCN)
    ])

    _breaker = Breaker(mrid="breaker", terminals=[
        Terminal(mrid="breaker_t1", phases=PhaseCode.ABCN),
        Terminal(mrid="breaker_t2", phases=PhaseCode.BN)
    ])

    _lv_line = AcLineSegment(mrid="lv_line", terminals=[
        Terminal(mrid="lv_line_t1", phases=PhaseCode.BN),
        Terminal(mrid="lv_line_t2", phases=PhaseCode.BN)
    ])

    _ec = EnergyConsumer(mrid="ec", terminals=[
        Terminal(mrid="ec_t", phases=PhaseCode.BN)
    ])

    # Now we add the objects and their terminals to the network
    network = NetworkService()
    for io in (_es, _hv_line, _hv_fuse, _tx, _lv_fuse, _breaker, _lv_line, _ec):
        network.add(io)  # add the object
        for terminal in io.terminals:  # iterate over Terminals
            network.add(terminal)  # add them too

    # Power grids aren't much use if the equipment in them isn't connected to anything,
    # Lets connect them.
    network.connect_terminals(network['es_t'], network['hv_line_t1'])
    network.connect_terminals(network['hv_line_t2'], network['hv_fuse_t1'])
    network.connect_terminals(network['hv_fuse_t2'], network['tx_t1'])
    network.connect_terminals(network['tx_t2'], network['lv_fuse_t1'])
    network.connect_terminals(network['lv_fuse_t2'], network['breaker_t1'])
    network.connect_terminals(network['breaker_t2'], network['lv_line_t1'])
    network.connect_terminals(network['lv_line_t2'], network['ec_t'])

    return network


def fancy_print_connectivity_result(_connectivity_result: ConnectivityResult):
    print(f"\t{_connectivity_result.from_terminal} to {_connectivity_result.to_terminal}")

    terminal_str_len = len(str(_connectivity_result.from_terminal))
    for core_path in _connectivity_result.nominal_phase_paths:
        print(f"\t{core_path.from_phase.name:>{terminal_str_len}}----{core_path.to_phase.name}")


def fancy_print_connected_equipment(equipment: ConductingEquipment, phases=None):
    if phases:
        print(f"Connectivity results for {equipment} on phases {phases}:")
    else:
        print(f"Connectivity results for {equipment}:")
    for _connectivity_result in connected_equipment(equipment, phases):
        fancy_print_connectivity_result(_connectivity_result)
    print()


if __name__ == '__main__':
    # This example explores how to examine the immediate connectivity of equipment.
    # We will build a simple, linear network to examine:
    n = build_network()

    # Get references to the ConductingEquipment we are interested in from the network
    tx = n['tx']
    breaker = n['breaker']
    lv_fuse_t2 = n['lv_fuse_t2']

    # connected_equipment(equipment, phases) will get all connections between equipment cores
    # matching one of the requested phases. The connected equipment does not need to connect
    # via all specified phases to appear in the list of connectivity results.
    fancy_print_connected_equipment(tx)
    fancy_print_connected_equipment(tx, phases=PhaseCode.N)
    fancy_print_connected_equipment(breaker, phases=PhaseCode.BC)

    # connected_terminals is essentially connected_equipment where only one terminal is considered.
    print(f"Connectivity results for terminal {lv_fuse_t2} on phases {PhaseCode.ACN}:")
    for connectivity_result in connected_terminals(lv_fuse_t2, PhaseCode.ACN):
        fancy_print_connectivity_result(connectivity_result)
