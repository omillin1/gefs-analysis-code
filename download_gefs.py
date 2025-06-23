###### Import modules ######
import xarray as xr
import fsspec
from glob import glob
import os
from tqdm import tqdm
import pandas as pd
from argparse import ArgumentParser

parser = ArgumentParser(description='Processes and downloads a run from GEFS heightAboveGround variables 31-member and 16-day lead data',
                        epilog='Example: download_gefs.py -d 20250621 -i 00 -v t2m -n 2t')
parser.add_argument('-d','--date',nargs=1,type=str,help='Date',default='20250621')
parser.add_argument('-i','--init',nargs=1,type=str,help='Hour',default='00')
parser.add_argument('-v','--variable',nargs=1,type=str,help='Variable name',default='t2m')
parser.add_argument('-n','--name',nargs=1,type=str,help='Variable indicator',default='2t')
args = parser.parse_args()

##### Set up GEFS run details ######
members = ["gec00"] + [f"gep{str(i).zfill(2)}" for i in range(1, 31)][:5] # Ensemble member labels.
hours = [f"f{str(h).zfill(3)}" for h in range(0, 243, 3)] + [f"f{str(h).zfill(3)}" for h in range(246, 385, 6)] # Hour labels.
# S3 base path (most common data prgb2a).
base_path = f"s3://noaa-gefs-pds/gefs.{args.date[0]}/{args.init[0]}/atmos/pgrb2ap5"
# Now set cache path.
cache_path = f'/share/data1/Students/ollie/GEFS_Test/Cache/'

###### Now loop through hours and members to get ensemble data ######
member_datasets = [] # List to store member datasets in.
for member in tqdm(members): # Loop through members.
    hourly_datasets = [] # List to store each step datasets in.
    for hour in hours: # Loop thrugh hours.
        # Get the URL for the S3 bucket.
        uri = f"filecache::s3://noaa-gefs-pds/gefs.{args.date[0]}/{args.init[0]}/atmos/pgrb2ap5/{member}.t{args.init[0]}z.pgrb2a.0p50.{hour}"
        # Get the file using fsspec.
        file = fsspec.open_local(uri, s3={'anon': True}, filecache={'cache_storage':cache_path})
        # Read the data with xarray.
        ds = xr.open_dataset(file, decode_timedelta = True, engine="cfgrib", backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround","shortName": f"{args.name[0]}"}})
        # Now assign coords and drop coords not being used. Also converts to valid_time.
        ds = ds.assign_coords(valid_time = ds['time'] + ds['step']).drop_vars(['time', 'step', 'heightAboveGround'])
        # Append to hourly datasets.
        hourly_datasets.append(ds)
    # Concat all valid time datasets for that member.
    member_ds = xr.concat(hourly_datasets, dim="valid_time")
    # Append this to list for ensemble members.
    member_datasets.append(member_ds)
    # Clear the cache to prevent large memory build-up.
    print("Clearing cache")
    files = glob(f"/share/data1/Students/ollie/GEFS_Test/Cache/*")
    for f in files:
        os.remove(f)

# Now concat along member so the data has dimension (valid_time, member, latitude, longitude).
final_data = xr.concat(member_datasets, dim = "number").transpose("valid_time", "number", "latitude", "longitude").isel(valid_time=slice(0, -1))

# Daily average.
daily_mean = final_data.resample(valid_time="1D").mean(skipna=True)

# Now save the data as an nc file.
# Set encodings for the nc file
encoding = {args.variable[0]: {"dtype": "float32",
                "complevel": 9,
                "zlib": True
                },
            'valid_time': {"dtype": "int32",
                "complevel": 9,
                "zlib": True,
                "calendar": "gregorian",
                "units":f"days since 1900-01-01"
                },
            'number': {"dtype": "int64",
                "complevel": 9,
                "zlib": True,
                },
            'latitude': {"dtype": "float64",
                "complevel": 9,
                "zlib": True,
                },
            'longitude': {"dtype": "float64",
                "complevel": 9,
                "zlib": True,
                },
                
                }
# Save nc file.
daily_mean.to_netcdf(f"/share/data1/Students/ollie/GEFS_Test/GEFS_{args.date[0]}_{args.init[0]}z_{args.variable[0]}.nc", format='NETCDF4_CLASSIC', encoding = encoding)
