#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import sys

from zepben.eas import FeederLoadAnalysisInput, Mutation, IngestorConfigInput

from zepben.examples.utils import eas_client_from_config

# This example kicks off an ingest of a network model that has already been uploaded to blob storage.
# See docs/docs/network_model_ingest.mdx for a full walkthrough, including how to prepare the model
# files in blob storage before running this script.

with open("config.json") as f:
    c = json.loads(f.read())["eas"]


async def main(argv):
    print("Connecting to EAS..")
    eas_client = eas_client_from_config(c, asynchronous=True)
    print("Connection established..")
    # Kick off a network model ingest.
    #
    # "dataStorePath" is the name of a folder in your blob storage, relative to the root
    # directory that Zepben configures once per environment for network model imports
    # (e.g. if that root is "data/inputs/models", a value of "full_network_20251015" here
    # points at "data/inputs/models/full_network_20251015"). The folder must already contain
    # the model files to ingest.
    #
    # The naming convention and internal formatting of these folders is customer-specific -
    # Zepben will provide you with examples tailored to your environment. A common workflow is
    # to copy an existing folder and only change what you need (e.g. tap settings), keeping the
    # rest of the formatting the same.

    execute_ingest = await eas_client.mutation(
        Mutation.execute_ingestor(
            run_config=[IngestorConfigInput(
                key="dataStorePath",
                value="<example_file_path>"
            )]
        )
    )

    print(f"Network model ingest: {execute_ingest['data']}")

    await eas_client.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv))
