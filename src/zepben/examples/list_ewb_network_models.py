#  Copyright 2024 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.
import tempfile

from zepben.evolve import EwbDataFilePaths, DatabaseType
from pathlib import Path
from datetime import date

# This example can either create its own temporary directories or be use an existing EWB data path.
create_temp_files = True

if create_temp_files:
    print("Creating temporary database directories for example...")
    data_path = Path(tempfile.gettempdir())
    network_list = ["2012-09-10", "2021-06-14", "2023-06-13", "2023-07-10", "2023-09-09", "2023-11-10"]
    customer_list = ["2008-01-01", "2009-01-01", "2010-01-01", "2011-01-01", "2012-09-10"]
    for to_create in network_list:
        new_dir = data_path.joinpath(to_create)
        new_dir.mkdir(exist_ok=True)
        new_dir.joinpath(f"{to_create}-network-model.sqlite").touch()
    for to_create in customer_list:
        new_dir = data_path.joinpath(to_create)
        new_dir.mkdir(exist_ok=True)
        new_dir.joinpath(f"{to_create}-customers.sqlite").touch()
else:
    data_path = Path("</path/to/ewb/data/>")
    network_list = None
    customer_list = None

# Initialize EwbDataFilePaths with the EWB data directory
ewb_data = EwbDataFilePaths(data_path)

# List all the dates for which exist network databases in the data path
list_of_available_dates = ewb_data.get_network_model_databases()
print(f"\nAll network databases in data directory ({ewb_data.base_dir}):")
for available_date in list_of_available_dates:
    print(f"{available_date.isoformat()}")

# Find the first date for which exists a customer database before 2011-09-10
closest_date_before = ewb_data.find_closest(DatabaseType.CUSTOMER, target_date=date(2011, 9, 10))
print(f"\nThe last customer database before 2011-09-10: {closest_date_before.isoformat() if closest_date_before is not None else closest_date_before}")

if create_temp_files:
    if network_list is not None and customer_list is not None:
        for to_cleanup in network_list:
            date_dir = data_path.joinpath(to_cleanup)
            date_dir.joinpath(f"{to_cleanup}-network-model.sqlite").unlink()
        for to_cleanup in customer_list:
            date_dir = data_path.joinpath(to_cleanup)
            date_dir.joinpath(f"{to_cleanup}-customers.sqlite").unlink()
        for to_cleanup in customer_list + network_list:
            date_dir = data_path.joinpath(to_cleanup)
            if date_dir.exists():
                date_dir.rmdir()
        print("\nTemporary files successfully cleaned up.")
    else:
        print("\nUnexpected issue while attempting to cleanup temporary files.")

