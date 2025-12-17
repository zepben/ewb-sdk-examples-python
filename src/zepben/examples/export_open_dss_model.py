#  Copyright 2025 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import json
from datetime import datetime

from zepben.eas.client.opendss import OpenDssConfig
from zepben.eas.client.work_package import GeneratorConfig, ModelConfig, FeederScenarioAllocationStrategy, SolveConfig, RawResultsConfig, \
    MeterPlacementConfig, SwitchMeterPlacementConfig, SwitchClass
from zepben.eas import EasClient, TimePeriod, FixedTime
from time import sleep
import requests


with open("config.json") as f:
    c = json.loads(f.read())


def wait_for_export(eas_client: EasClient, model_id: int):
    # Wait for OpenDss model export to complete
    wait_limit_seconds = 3000
    step_seconds = 2
    total = 0
    print(f"Waiting for model generation ({wait_limit_seconds} seconds) ", end='')
    # Retrieve the model information for the model we just requested
    model = eas_client.get_opendss_model(model_id)
    while model["state"] == "CREATION":
        try:
            model = eas_client.get_opendss_model(model_id)
            print(".", end='')
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
        access_token=c["access_token"]
    )

    # Run an opendss export
    print("Sending OpenDss model export request to EAS")

    response = eas_client.run_opendss_export(
        OpenDssConfig(
            scenario="base",
            year=2025,
            feeder="<FEEDER_MRID>",
            load_time=TimePeriod(
                start_time=datetime.fromisoformat("2024-04-01T00:00"),
                end_time=datetime.fromisoformat("2025-04-01T00:00")
            ),
            # For fixed time export example, pass load_time a FixedTime object
            #load_time=FixedTime(
            #    time=datetime.fromisoformat("2024-04-01T00:00")
            #),
            generator_config=GeneratorConfig(
                model=ModelConfig(
                    meter_placement_config=MeterPlacementConfig(
                        feeder_head=True,
                        dist_transformers=True,
                        # Include meters for any switch that has a name that starts with 'LV Circuit Head' and is a Fuse or Disconnector
                        switch_meter_placement_configs=[SwitchMeterPlacementConfig(
                            meter_switch_class=SwitchClass.DISCONNECTOR,
                            name_pattern="LV Circuit Head.*"
                        ), SwitchMeterPlacementConfig(
                            meter_switch_class=SwitchClass.FUSE,
                            name_pattern="LV Circuit Head.*"
                        )]
                    ),
                    load_vmax_pu=1.2,
                    load_vmin_pu=0.8,
                    p_factor_base_exports=-1,
                    p_factor_base_imports=1,
                    p_factor_forecast_pv=0.98,
                    fix_single_phase_loads=True,
                    max_single_phase_load=15000.0,
                    max_load_service_line_ratio=1.0,
                    max_load_lv_line_ratio=2.0,
                    max_load_tx_ratio=2.0,
                    max_gen_tx_ratio=4.0,
                    fix_overloading_consumers=True,
                    fix_undersized_service_lines=True,
                    feeder_scenario_allocation_strategy=FeederScenarioAllocationStrategy.ADDITIVE,
                    closed_loop_v_reg_enabled=True,
                    closed_loop_v_reg_set_point=0.9825,
                    seed=123,
                ),
                solve=SolveConfig(step_size_minutes=30.0),
                raw_results=RawResultsConfig(True, True, True, True, True)
            ),
            model_name=export_file_name,
            is_public=True
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
