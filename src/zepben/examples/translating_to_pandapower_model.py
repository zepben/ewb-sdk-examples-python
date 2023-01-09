#  Copyright 2023 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import logging

from pp_creators.basic_creator import BasicPandaPowerNetworkCreator
from zepben.examples.ieee_13_node_test_feeder import network

logger = logging.getLogger(__name__)


async def main():
    result = await BasicPandaPowerNetworkCreator(logger=logger).create(network)
    print(f"Translation successful: {result.was_successful}")


if __name__ == "__main__":
    asyncio.run(main())
