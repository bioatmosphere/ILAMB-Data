"""
Converts xlsx file of soil carbon data into a CF-compliant netCDF4 file.

Methods:

TODO:
- calculate response ratio (RR)
- add uncertainty

"""

import time
import argparse
import glob
import re
import xlrd

import pandas as pd
import numpy as np
import xarray as xr


import cftime as cf
from netCDF4 import Dataset
from cf_units import Unit


def download_file(remote_source: str, output_path: str = "_raw") -> str:
    """Downloads a file from a remote source to a local path.

    Left off as a utility function for downloading files.
    
    Args:
        remote_source (str): The URL of the file to download.
        output_path (str): The local directory where the file will be saved.
    Returns:
        str: The local path to the downloaded file.
    """

    output_path = Path(output_path)
    if not output_path.is_dir():
        output_path.mkdir(parents=True, exist_ok=True)
    local_source = output_path / os.path.basename(remote_source).split("?")[0]
    if os.path.isfile(local_source):
        return local_source
    resp = requests.get(remote_source, stream=True, timeout=10)
    resp.raise_for_status()
    with open(local_source, "wb") as fdl:
        with tqdm(
            total=int(resp.headers["Content-Length"]),
            unit="B",
            unit_scale=True,
            desc=str(local_source),
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
                    pbar.update(len(chunk))
    return local_source


# Read the xlsx file into a dataframe
# Basic reading of an Excel file
df = pd.read_excel(
    "soilC.xlsx",
    sheet_name="combined",  # specify sheet name or index (0 by default)
    header=0,            # row to use as column names (0-based)
    skiprows=None,       # number of rows to skip at start of file
    usecols=None,        # columns to read (e.g., "A:C" or [0,1,2])
    na_values=["NA", "missing"],  # additional NA/NaN strings
    #dtype={"column_name": "float64"}  # specify data types for columns
)

# Drop rows where all values are NaN
df_cleaned = df.dropna(how='all')

# Calculate the response ratio (RR)
df_cleaned.loc[:,'soc_rr'] = np.log(df_cleaned['SOC.elev (g/m2)']/df_cleaned['SOC.amb (g/m2)'])

ds = df_cleaned.to_xarray()
ds['soc_rr'].attrs = {"standard_name":"soil_organic_carbon_response_ratio",
                     "units":"unitness"}
ds = ds.rename({"Latitude":"lat"})
ds = ds.rename({"Longitude":"lon"})

ds['lat'].attrs = {"standard_name":"latitude",
                  "units":"degrees_north"}
ds['lon'].attrs = {"standard_name":"longitude",
                  "units":"degrees_east"}

attrs = {
    "title": "Meta-analysis of soil carbon data",
    "version": 2025,
    "institutions": "OU-ORNL",
    "source": "Data downloaded from ...",
    "history": " ",
    "references": " ",
}

out = ds['soc_rr'].to_dataset(name='soc_rr')
out["lat"] = ("site",ds['lat'].data)
out["lon"] = ("site",ds['lon'].data)
out['soc_rr'].attrs["coordinates"] = "lat lon"
out.attrs = attrs
out.to_netcdf("soc_rr.nc")