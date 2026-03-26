#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
from datetime import datetime

from zepben.eas import EasClient, OpenDssModelInput, OpenDssModulesConfigInput, OpenDssModelGenerationSpecInput, OpenDssModelOptionsInput, \
    OpenDssCommonConfigInput, HcGeneratorConfigInput, TimePeriodInput, FixedTimeInput, HcModelConfigInput, HcSolveConfigInput, HcNodeLevelResultsConfigInput, \
    HcRawResultsConfigInput, HcMeterPlacementConfigInput, HcSwitchMeterPlacementConfigInput, HcSwitchClass, HcFeederScenarioAllocationStrategy
from time import sleep
import requests


with open("config.json") as f:
    c = json.loads(f.read())


def wait_for_export(eas_client: EasClient, model_id: int):
    # Wait for OpenDss model export to complete
    wait_limit_seconds = 3000
    step_seconds = 2
    total = 0
    print(f"Waiting for model generation ({wait_limit_seconds} seconds) ", end='', flush=True)
    # Retrieve the model information for the model we just requested
    model = eas_client.get_opendss_model(model_id)
    while model["state"] == "CREATION":
        try:
            model = eas_client.get_opendss_model(model_id)
            print(".", end='', flush=True)
            sleep(step_seconds)
            total += step_seconds
            if total > wait_limit_seconds:
                raise TimeoutError("Timed out waiting for model export to complete.")
        except Exception as e:
            if isinstance(e, TimeoutError):
                raise e
            else:
                print(f"Failed retrieving model export status: {e}")
                print(f"Retrying in {step_seconds} secondds...")
                sleep(step_seconds)


def download_generated_model(eas_client: EasClient, output_file_name: str, model_id: int):
    try:
        url = eas_client.get_opendss_model_download_url(model_id)
    except Exception as e:
        print()
        print(f"Download failed, model failed to generate: {e}")
        return

    print(f"\nURL (30 second expiry): {url}", )

    file_name = f"{output_file_name}-{model_id}.zip"
    print(f"Downloading model zip to: {file_name}")

    try:
        with open(file_name, mode="wb") as file:
            file.write(requests.get(url).content)
        print("Download complete.")
    except Exception as error:
        print(error)
        print("Download failed. Model may have failed to generate.")


def open_dss_export(export_file_name: str):
    eas_client = EasClient(
        host=c["host"],
        port=c["rpc_port"],
        access_token=c["access_token"],
    )

    # Run an opendss export
    print("Sending OpenDss model export request to EAS")

    response = eas_client.run_opendss_export(
        OpenDssModelInput(
            generationSpec=OpenDssModelGenerationSpecInput(
                modelOptions=OpenDssModelOptionsInput(
                    scenario="base",
                    year=2025,
                    feeder="<FEEDER_MRID>",
                ),
                modulesConfiguration=OpenDssModulesConfigInput(
                    common=OpenDssCommonConfigInput(
                        timePeriod=TimePeriodInput(
                            startTime=datetime.fromisoformat("2024-04-01T00:00"),
                            endTime=datetime.fromisoformat("2025-04-01T00:00")
                        ),
                        # For fixed time export example, pass load_time a FixedTimeInput object
                        # fixedTime=FixedTimeInput(
                        #     loadTime=datetime.fromisoformat("2024-04-01T00:00")
                        # )
                    ),
                    generator=HcGeneratorConfigInput(
                        model=HcModelConfigInput(
                            meterPlacementConfig=HcMeterPlacementConfigInput(
                                feederHead=True,
                                distTransformers=True,
                                # Include meters for any switch that has a name that starts with 'LV Circuit Head' and is a Fuse or Disconnector
                                switchMeterPlacementConfigs=[
                                    HcSwitchMeterPlacementConfigInput(
                                        meterSwitchClass=HcSwitchClass.DISCONNECTOR,
                                        namePattern="LV Circuit Head.*"
                                    ), HcSwitchMeterPlacementConfigInput(
                                        meterSwitchClass=HcSwitchClass.FUSE,
                                        namePattern="LV Circuit Head.*"
                                    )
                                ]
                            ),
                            loadVMaxPu=1.2,
                            loadVMinPu=0.8,
                            pFactorBaseExports=-1,
                            pFactorBaseImports=1,
                            pFactorForecastPv=0.98,
                            fixSinglePhaseLoads=True,
                            maxSinglePhaseLoad=15000.0,
                            maxLoadServiceLineRatio=1.0,
                            maxLoadLvLineRatio=2.0,
                            maxLoadTxRatio=2.0,
                            maxGenTxRatio=4.0,
                            fixOverloadingConsumers=True,
                            fixUndersizedServiceLines=True,
                            feederScenarioAllocationStrategy=HcFeederScenarioAllocationStrategy.ADDITIVE,
                            closedLoopVRegEnabled=True,
                            closedLoopVRegSetPoint=0.9825,
                            seed=123,

                        ),
                        solve=HcSolveConfigInput(
                            stepSizeMinutes=30
                        ),
                        rawResults=HcRawResultsConfigInput(
                            energyMetersRaw=True,
                            energyMeterVoltagesRaw=True,
                            overloadsRaw=True,
                            resultsPerMeter=True,
                            voltageExceptionsRaw=True,
                        ),
                    ),
                )
            ),
            isPublic=True,
            modelName=export_file_name,
        )
    )
    print(f"Raw 'run_opendss_export' response: '{response}'")
    model_id = response["data"]["createOpenDssModel"]
    print(f"New OpenDss model export id: {model_id}")

    try:
        wait_for_export(eas_client, int(model_id))

        # Request a download URL from EAS and download to a local file
        download_generated_model(eas_client, export_file_name, int(model_id))

    except TimeoutError:
        print("\nERROR: Timed out waiting for model export to complete.")

    eas_client.close()


if __name__ == "__main__":
    open_dss_export(f"test_export_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
