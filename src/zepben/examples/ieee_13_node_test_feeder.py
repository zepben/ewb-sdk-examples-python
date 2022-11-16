#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import AcLineSegment, Disconnector, PowerTransformer, TransformerFunctionKind, NetworkService, Terminal, PowerTransformerEnd

vr_650_632_t1, vr_650_632_t2 = Terminal(), Terminal()
vr_650_632_e1 = PowerTransformerEnd(terminal=vr_650_632_t1)
vr_650_632_e2 = PowerTransformerEnd(terminal=vr_650_632_t2)
vr_650_632 = PowerTransformer(function=TransformerFunctionKind.voltageRegulator, terminals=[vr_650_632_t1, vr_650_632_t2],
                              power_transformer_ends=[vr_650_632_e1, vr_650_632_e2])

l_632_645_t1, l_632_645_t2 = Terminal(), Terminal()
l_632_645 = AcLineSegment(length=500, terminals=[l_632_645_t1, l_632_645_t2])

l_632_633_t1, l_632_633_t2 = Terminal(), Terminal()
l_632_633 = AcLineSegment(length=500, terminals=[l_632_633_t1, l_632_633_t2])

tx_633_634_t1, tx_633_634_t2 = Terminal(), Terminal()
tx_633_634_e1 = PowerTransformerEnd(terminal=tx_633_634_t1)
tx_633_634_e2 = PowerTransformerEnd(terminal=tx_633_634_t2)
tx_633_634 = PowerTransformer(terminals=[tx_633_634_t1, tx_633_634_t2], power_transformer_ends=[tx_633_634_e1, tx_633_634_e2])

l_645_646_t1, l_645_646_t2 = Terminal(), Terminal()
l_645_646 = AcLineSegment(length=300, terminals=[l_645_646_t1, l_645_646_t2])

l_650_632_t1, l_650_632_t2 = Terminal(), Terminal()
l_650_632 = AcLineSegment(length=2000, terminals=[l_650_632_t1, l_650_632_t2])

l_684_652_t1, l_684_652_t2 = Terminal(), Terminal()
l_684_652 = AcLineSegment(length=800, terminals=[l_684_652_t1, l_684_652_t2])

l_632_671_t1, l_632_671_t2 = Terminal(), Terminal()
l_632_671 = AcLineSegment(length=2000, terminals=[l_632_671_t1, l_632_671_t2])

l_671_684_t1, l_671_684_t2 = Terminal(), Terminal()
l_671_684 = AcLineSegment(length=300, terminals=[l_671_684_t1, l_671_684_t2])

l_671_680_t1, l_671_680_t2 = Terminal(), Terminal()
l_671_680 = AcLineSegment(length=1000, terminals=[l_671_680_t1, l_671_680_t2])

sw_671_692_t1, sw_671_692_t2 = Terminal(), Terminal()
sw_671_692 = Disconnector(terminals=[sw_671_692_t1, sw_671_692_t2])

l_684_611_t1, l_684_611_t2 = Terminal(), Terminal()
l_684_611 = AcLineSegment(length=300, terminals=[l_684_611_t1, l_684_611_t2])

l_692_675_t1, l_692_675_t2 = Terminal(), Terminal()
l_692_675 = AcLineSegment(length=500, terminals=[l_692_675_t1, l_692_675_t2])

network = NetworkService()
for io in [vr_650_632, vr_650_632_t1, vr_650_632_t2, vr_650_632_e1, vr_650_632_e2, l_632_645, l_632_645_t1, l_632_645_t2, l_632_633, l_632_633_t1, l_632_633_t2,
           tx_633_634, tx_633_634_t1, tx_633_634_t2, tx_633_634_e1, tx_633_634_e2, l_645_646, l_645_646_t1, l_645_646_t2, l_650_632, l_650_632_t1, l_650_632_t2,
           l_684_652, l_684_652_t1, l_684_652_t2, l_632_671, l_632_671_t1, l_632_671_t2, l_671_684, l_671_684_t1, l_671_684_t2, l_671_680, l_671_680_t1,
           l_671_680_t2, sw_671_692, sw_671_692_t1, sw_671_692_t2, l_684_611, l_684_611_t1, l_684_611_t2, l_692_675, l_692_675_t1, l_692_675_t2]:
    network.add(io)

# Complete 630-632 regulator + line
network.connect_terminals(vr_650_632_t2, l_650_632_t1)

# Node 632
network.connect_terminals(l_650_632_t2, l_632_633_t1)
network.connect_terminals(l_650_632_t2, l_632_645_t1)
network.connect_terminals(l_650_632_t2, l_632_671_t1)

# Node 633
network.connect_terminals(l_632_633_t2, tx_633_634_t1)

# Node 645
network.connect_terminals(l_632_645_t2, l_645_646_t1)

# Node 671
network.connect_terminals(l_632_671_t2, l_671_680_t1)
network.connect_terminals(l_632_671_t2, l_671_684_t1)
network.connect_terminals(l_632_671_t2, sw_671_692_t1)

# Node 684
network.connect_terminals(l_671_684_t2, l_684_611_t1)
network.connect_terminals(l_671_684_t2, l_684_652_t1)

# Node 692
network.connect_terminals(sw_671_692_t2, l_692_675_t1)
