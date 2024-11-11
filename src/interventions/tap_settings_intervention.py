#  Copyright 2024 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import os
import pandas as pd

results_file_path = "results/interventions_before_updating_taps_30-10-2024.csv"
current_tap_settings_file_path = "taps/dist-tx-taps-zepben.csv"

results_file_name = os.path.basename(results_file_path)
year = results_file_name.split('-')[0]
updated_tap_settings_file_path = f"taps/interventions_taps.csv"

df_results = pd.read_csv(results_file_path)
df_taps = pd.read_csv(current_tap_settings_file_path)

df_results['conducting_equipment_mrid'] = df_results['conducting_equipment_mrid'].astype(str)
df_taps['ID'] = df_taps['ID'].astype(str)

filtered_results_df = df_results[(df_results['mz_type'] == 'TRANSFORMER') & (df_results['season'] == 'yearly')]

csv_lookup = filtered_results_df.set_index('conducting_equipment_mrid').to_dict('index')

updated_tap_count = 0
threshold = 0.1
X = 100
Y = 100
large_swing_over_voltage_case_1_eligible_ids = []
large_swing_over_voltage_case_2_eligible_ids = []
large_swing_under_voltage_case_1_eligible_ids = []
large_swing_under_voltage_case_2_eligible_ids = []
small_swing_over_voltage_case_1_eligible_ids = []
small_swing_over_voltage_case_2_eligible_ids = []
small_swing_under_voltage_case_1_eligible_ids = []
small_swing_under_voltage_case_2_eligible_ids = []


def prioritize_tap_selection(row, transformer_data, category):
    tap_weighting_factor_high = abs(transformer_data.get('v99_avg_section_voltage', 0) - 1) * transformer_data.get('voltage_over_limit_hours', 0)
    tap_weighting_factor_low = abs(transformer_data.get('v1_avg_section_voltage', 0) - 1) * transformer_data.get('voltage_under_limit_hours', 0)
    tap_weighting_factor = tap_weighting_factor_high - tap_weighting_factor_low
    print(f"TWF is for {row['ID']} {tap_weighting_factor}")
    current_tap_position = row['Tap Position']
    if tap_weighting_factor < -10:
        if category == "large":
            large_swing_under_voltage_case_1_eligible_ids.append(row['ID'])
        else:
            small_swing_under_voltage_case_1_eligible_ids.append(row['ID'])
        print(f"Prioritizing algorithm: Current Tap {current_tap_position} changed to {min(8, current_tap_position + 1)}")
        return min(8, current_tap_position + 1)
    elif tap_weighting_factor > 10:
        if category == "large":
            large_swing_over_voltage_case_1_eligible_ids.append(row['ID'])
        else:
            small_swing_over_voltage_case_1_eligible_ids.append(row['ID'])
        print(f"Prioritizing algorithm: Current Tap {current_tap_position} changed to {max(1, current_tap_position - 1)}")
        return max(1, current_tap_position - 1)
    else:
        print(f"Prioritizing algorithm: No Tap Change")
        return current_tap_position


def adjust_tap_position(row):
    global updated_tap_count
    current_tap_position = row['Tap Position']

    transformer_data = csv_lookup.get(row['ID'])

    if transformer_data:
        voltage_delta_avg = transformer_data.get('voltage_delta_avg')
        voltage_over_limit_hours = transformer_data.get('voltage_over_limit_hours')
        voltage_under_limit_hours = transformer_data.get('voltage_under_limit_hours')
        v1_avg_section_voltage = transformer_data.get('v1_avg_section_voltage')
        v99_avg_section_voltage = transformer_data.get('v99_avg_section_voltage')

        if voltage_delta_avg is not None and voltage_delta_avg > threshold:
            if (voltage_over_limit_hours is not None and voltage_over_limit_hours > X) and (
            voltage_under_limit_hours is not None and voltage_under_limit_hours > Y):
                new_tap_position = prioritize_tap_selection(row, transformer_data, category="large")
            elif voltage_over_limit_hours > X and v99_avg_section_voltage > 1.1:
                large_swing_over_voltage_case_2_eligible_ids.append(row['ID'])
                new_tap_position = max(1, current_tap_position - 1)
            elif voltage_under_limit_hours > Y and v1_avg_section_voltage < 0.91:
                large_swing_under_voltage_case_2_eligible_ids.append(row['ID'])
                new_tap_position = min(8, current_tap_position + 1)
            else:
                new_tap_position = current_tap_position
        else:
            if v1_avg_section_voltage > 1 and v99_avg_section_voltage > 1.1:
                if voltage_over_limit_hours > X and voltage_under_limit_hours < Y:
                    small_swing_over_voltage_case_1_eligible_ids.append(row['ID'])
                    new_tap_position = max(1, current_tap_position - 2)
                else:
                    new_tap_position = prioritize_tap_selection(row, transformer_data, "small")
            elif (0.97 < v1_avg_section_voltage <= 1) and v99_avg_section_voltage > 1.1:
                if voltage_over_limit_hours > X and voltage_under_limit_hours < Y:
                    small_swing_over_voltage_case_2_eligible_ids.append(row['ID'])
                    new_tap_position = max(1, current_tap_position - 1)
                else:
                    new_tap_position = current_tap_position
            elif v1_avg_section_voltage <= 0.91 and v99_avg_section_voltage < 1.075:
                if voltage_under_limit_hours > Y and voltage_over_limit_hours < X:
                    small_swing_under_voltage_case_1_eligible_ids.append(row['ID'])
                    new_tap_position = min(8, current_tap_position + 2)
                else:
                    new_tap_position = prioritize_tap_selection(row, transformer_data, "small")
            elif v1_avg_section_voltage < 0.94 and (1.075 <= v99_avg_section_voltage < 1.1):
                if voltage_under_limit_hours > Y and voltage_over_limit_hours < X:
                    small_swing_under_voltage_case_2_eligible_ids.append(row['ID'])
                    new_tap_position = min(8, current_tap_position + 1)
                else:
                    new_tap_position = current_tap_position
            else:
                new_tap_position = current_tap_position

        if new_tap_position != current_tap_position:
            updated_tap_count += 1
        return new_tap_position

    # If no matching transformer data, return the current tap position unchanged
    return current_tap_position


# Apply the tap position adjustment function to each row in the CSV
df_taps['Tap Position'] = df_taps.apply(adjust_tap_position, axis=1)

print(f"Large Swing Over Voltage Case 1: {len(large_swing_over_voltage_case_1_eligible_ids)}")
print(f"Large Swing Over Voltage Case 2: {len(large_swing_over_voltage_case_2_eligible_ids)}")
print(f"Large Swing Under Voltage Case 1: {len(large_swing_under_voltage_case_1_eligible_ids)}")
print(f"Large Swing Under Voltage Case 2: {len(large_swing_under_voltage_case_2_eligible_ids)}")

print(f"Small Swing Over Voltage Case 1: {len(small_swing_over_voltage_case_1_eligible_ids)}")
print(f"Small Swing Over Voltage Case 2: {len(small_swing_over_voltage_case_2_eligible_ids)}")
print(f"Small Swing Under Voltage Case 1: {len(small_swing_under_voltage_case_1_eligible_ids)}")
print(f"Small Swing Under Voltage Case 2: {len(small_swing_under_voltage_case_2_eligible_ids)}")

print(f"Large Swing Over Voltage Case 1: {large_swing_over_voltage_case_1_eligible_ids}")
print(f"Large Swing Over Voltage Case 2: {large_swing_over_voltage_case_2_eligible_ids}")
print(f"Large Swing Under Voltage Case 1: {large_swing_under_voltage_case_1_eligible_ids}")
print(f"Large Swing Under Voltage Case 2: {large_swing_under_voltage_case_2_eligible_ids}")

print(f"Small Swing Over Voltage Case 1: {small_swing_over_voltage_case_1_eligible_ids}")
print(f"Small Swing Over Voltage Case 2: {small_swing_over_voltage_case_2_eligible_ids}")
print(f"Small Swing Under Voltage Case 1: {small_swing_under_voltage_case_1_eligible_ids}")
print(f"Small Swing Under Voltage Case 2: {small_swing_under_voltage_case_2_eligible_ids}")

print(f"Total {updated_tap_count} transformers had their tap positions updated.")

# Save the updated CSV file with only the modified tap positions
df_taps.to_csv(updated_tap_settings_file_path, index=False)
print(f"Updated tap settings saved to {updated_tap_settings_file_path}")
