#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import AcLineSegment, Disconnector, PowerTransformer

l_632_645 = AcLineSegment(length=500)
l_632_633 = AcLineSegment(length=500)
tx_633_634 = PowerTransformer()
l_645_646 = AcLineSegment(length=300)
l_650_632 = AcLineSegment(length=2000)
l_684_652 = AcLineSegment(length=800)
l_632_671 = AcLineSegment(length=2000)
l_671_684 = AcLineSegment(length=300)
l_671_680 = AcLineSegment(length=1000)
sw_671_692 = Disconnector()
l_684_611 = AcLineSegment(length=300)
l_692_675 = AcLineSegment(length=500)
