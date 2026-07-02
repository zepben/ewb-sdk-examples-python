#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import asyncio
import json
import sys

from zepben.eas import FeederLoadAnalysisInput, EasClient, Mutation, IngestorConfigInput

with open("config.json") as f:
    c = json.loads(f.read())


async def main(argv):
    print("Connecting to EAS..")
    eas_client = EasClient(
        host=c["eas_host"],
        port=c["eas_port"],
        protocol=c["eas_protocol"],
        access_token=c["access_token"],
        verify_certificate=c.get("verify_certificate", True),
        ca_filename=c["ca_path"],
        asynchronous=True
    )
    print("Connection established..")
    # Kick off a network model ingest
    execute_ingest = await eas_client.mutation(
        Mutation.execute_ingestor(
            run_config=[IngestorConfigInput(
                key="dataStorePath",
                value="full_network_20251015"
            )]
        )
    )

    print(f"Network model ingest: {execute_ingest['data']}")

    await eas_client.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(sys.argv))
