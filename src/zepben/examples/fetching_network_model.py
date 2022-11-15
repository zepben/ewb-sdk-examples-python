#  Copyright 2022 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
from zepben.evolve import connect_tls, NetworkConsumerClient, Feeder, Equipment
from zepben.protobuf.nc.nc_requests_pb2 import INCLUDE_ENERGIZED_LV_FEEDERS


async def fetch_network():

    # Refer to `connecting_to_grpc_service` for examples of all connect functions.
    async with connect_tls("hostname", 1234) as channel:
        client = NetworkConsumerClient(channel)
        await client.get_equipment_container("feeder mRID", Feeder, include_energized_containers=INCLUDE_ENERGIZED_LV_FEEDERS)

        # client.service now contains all equipment in the feeder with mRID "feeder mRID", as well as equipment in the LV feeders energized by that feeder.
        print(client.service.len_of(Equipment))

        # Additional requests will continue adding novel objects to client.service
        await client.get_all_loops()
        print(client.service.len_of(Equipment))
