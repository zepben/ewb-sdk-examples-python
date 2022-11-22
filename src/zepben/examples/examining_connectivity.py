#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import EnergySource, AcLineSegment, Fuse, PowerTransformer, Breaker, EnergyConsumer, NetworkService, Terminal, connected_equipment, \
    ConductingEquipment, PhaseCode, connected_terminals, ConnectivityResult

# This example explores how to examine the immediate connectivity of equipment.
# We will build a simple, linear network to examine:

#    source     consumer
#      |           |
#     line       line
#      |           |
#     fuse      breaker
#      |           |
# transformer     fuse
#      |           |
#      +-----------+

es_t = Terminal(mrid="es-t")
es = EnergySource(mrid="es", terminals=[es_t])

hv_line_t1, hv_line_t2 = Terminal(mrid="hv_line_t1"), Terminal(mrid="hv_line_t2")
hv_line = AcLineSegment(mrid="hv_line", terminals=[hv_line_t1, hv_line_t2])

hv_fuse_t1, hv_fuse_t2 = Terminal(mrid="hv_fuse_t1"), Terminal(mrid="hv_fuse_t2")
hv_fuse = Fuse(mrid="hv_fuse", terminals=[hv_fuse_t1, hv_fuse_t2])

tx_t1, tx_t2 = Terminal(mrid="tx_t1"), Terminal(mrid="tx_t2", phases=PhaseCode.ABCN)
tx = PowerTransformer(mrid="tx", terminals=[tx_t1, tx_t2])

lv_fuse_t1, lv_fuse_t2 = Terminal(mrid="lv_fuse_t1", phases=PhaseCode.ABCN), Terminal(mrid="lv_fuse_t2", phases=PhaseCode.ABCN)
lv_fuse = Fuse(mrid="lv_fuse", terminals=[lv_fuse_t1, lv_fuse_t2])

breaker_t1, breaker_t2 = Terminal(mrid="breaker_t1", phases=PhaseCode.ABCN), Terminal(mrid="breaker_t2", phases=PhaseCode.BN)
breaker = Breaker(mrid="breaker", terminals=[breaker_t1, breaker_t2])

lv_line_t1, lv_line_t2 = Terminal(mrid="lv_line_t1", phases=PhaseCode.BN), Terminal(mrid="lv_line_t2", phases=PhaseCode.BN)
lv_line = AcLineSegment(mrid="lv_line", terminals=[lv_line_t1, lv_line_t2])

ec_t = Terminal(mrid="ec_t", phases=PhaseCode.BN)
ec = EnergyConsumer(mrid="ec", terminals=[ec_t])

network = NetworkService()
for io in [es_t, es, hv_line_t1, hv_line_t2, hv_line, hv_fuse_t1, hv_fuse_t2, hv_fuse, tx_t1, tx_t2, tx, lv_fuse_t1, lv_fuse_t2, lv_fuse, breaker_t1,
           breaker_t2, breaker, lv_line_t1, lv_line_t2, lv_line, ec_t, ec]:
    network.add(io)

network.connect_terminals(es_t, hv_line_t1)
network.connect_terminals(hv_line_t2, hv_fuse_t1)
network.connect_terminals(hv_fuse_t2, tx_t1)
network.connect_terminals(tx_t2, lv_fuse_t1)
network.connect_terminals(lv_fuse_t2, breaker_t1)
network.connect_terminals(breaker_t2, lv_line_t1)
network.connect_terminals(lv_line_t2, ec_t)


def fancy_print_connectivity_result(connectivity_result: ConnectivityResult):
    print(f"\t{connectivity_result.from_terminal} to {connectivity_result.to_terminal}")

    terminal_str_len = len(str(connectivity_result.from_terminal))
    for core_path in connectivity_result.nominal_phase_paths:
        print(f"\t{core_path.from_phase.name:>{terminal_str_len}}----{core_path.to_phase.name}")


def fancy_print_connected_equipment(equipment: ConductingEquipment, phases=None):
    if phases:
        print(f"Connectivity results for {equipment} on phases {phases}:")
    else:
        print(f"Connectivity results for {equipment}:")
    for connectivity_result in connected_equipment(equipment, phases):
        fancy_print_connectivity_result(connectivity_result)
    print()


# connected_equipment(equipment, phases) will get all connections between equipment cores matching one of the requested phases.
# The connected equipment does not need to connect via all specified phases to appear in the list of connectivity results.
fancy_print_connected_equipment(tx)
fancy_print_connected_equipment(tx, phases=PhaseCode.N)
fancy_print_connected_equipment(breaker, phases=PhaseCode.BC)

# connected_terminals is essentially connected_equipment where only one terminal is considered.
print(f"Connectivity results for terminal {lv_fuse_t2} on phases {PhaseCode.ACN}:")
for connectivity_result in connected_terminals(lv_fuse_t2, PhaseCode.ACN):
    fancy_print_connectivity_result(connectivity_result)
