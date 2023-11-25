import pandas as pd
import os
import requests
import gzip
import shutil
from datetime import datetime, timedelta
import dateutil.parser
from plot import plot_chain

CHAIN_FOLDER = "chain"
CHAIN_BASE_URL = "http://chain.physics.unb.ca/data/gps/ismr/"
OUTPUT_FOLDER = "figure"
CHAIN_HEADER = ["Week", "GPS TOW", "PRN", "RxStatus", "Az", "Elv", "L1 CNo", "S4", "S4 Cor", "1SecSigma",
    "3SecSigma", "10SecSigma", "30SecSigma", "60SecSigma", "Code-Carrier", "C-CStdev", "TEC45", 
    "TECRate45", "TEC30", "TECRate30", "TEC15", "TECRate15", "TEC0", "TECRate0", "L1 LockTime", 
    "ChanStatus", "L2 LockTime", "L2 CNo"]
NA_VALUES = [' nan', 'NaN', 'N/A', 'NA', 'NULL'] 

def read_chain(filenames: list[str], filenames_backup: list[str]):
    """
    Reads and combines data from a list of files.
    Assumes a specific data format as in the original MATLAB function.
    """
    all_data = None

    # Loop through each file
    for filename, filename_backup in zip(filenames, filenames_backup):
        try:
            # Read the data assuming a CSV format with a header
            data = pd.read_csv(
                os.path.join(CHAIN_FOLDER, filename), 
                header=0, 
                delimiter=',', 
                names=CHAIN_HEADER,
                na_values=NA_VALUES)

            # If this is the first file, initialize all_data with its contents
            if all_data is None:
                all_data = data
            else:
                # Otherwise, append the data from this file to all_data
                all_data = pd.concat([all_data, data], ignore_index=True)
        except FileNotFoundError:
            # Read the data assuming a CSV format with a header
            data = pd.read_csv(
                os.path.join(CHAIN_FOLDER, filename_backup), 
                header=None, 
                delimiter=',', 
                names=CHAIN_HEADER, 
                usecols=list(range(28)),
                na_values=NA_VALUES)

            # If this is the first file, initialize all_data with its contents
            if all_data is None:
                all_data = data
            else:
                # Otherwise, append the data from this file to all_data
                all_data = pd.concat([all_data, data], ignore_index=True)
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
            breakpoint()
            continue

    return all_data


def chain_main(filenames: list[str], filenames_backup: list[str], outputname: str, starttime=None, endtime=None):
    """Load chain data and plot
    """

    # Read and combine the data
    combined_data = read_chain(filenames, filenames_backup)
    combined_data['utc_times'] = pd.Series([gpstoutc(row['Week'],row['GPS TOW']) for index, row in combined_data.iterrows()])
    combined_data.set_index('utc_times', inplace=True)

    if starttime is not None and endtime is not None:
        combined_data = combined_data[datetime.strptime(starttime, '%Y-%m-%d/%H:%M:%S'): datetime.strptime(endtime, '%Y-%m-%d/%H:%M:%S')]
    
    # Accessing combined data
    # Assuming specific columns exist in the data
    combined_data['PRN'] = combined_data['PRN'].astype(int)
    combined_data['Elv'] = combined_data['Elv'].astype(float)
    combined_data['S4'] = combined_data['S4'].astype(float)
    combined_data['60SecSigma'] = combined_data['60SecSigma'].astype(float)
    combined_data['TEC0'] = combined_data['TEC0'].astype(float)
    combined_data['L1 CNo'] = combined_data['L1 CNo'].astype(float)
    combined_data['L2 CNo'] = combined_data['L2 CNo'].astype(float)

    # Filter based on elevation and get unique PRNs
    select = combined_data['Elv'] > 60.0
    elev_select = combined_data['PRN'][select].unique()

    # Convert times to minutes from a specific start time
    #time0 = times.iloc[0] + deltaT + 30.0
    #time_min = (times - time0) / 60.0

    # Plotting
    plot_chain(combined_data, elev_select, os.path.join(OUTPUT_FOLDER, outputname))
    
        

def chain_download(filenames: list[str], filenames_backup: list[str]):
    """
    Download chain data from http://chain.physics.unb.ca/data/gps/ismr/. 
    construct url with filnames
    filenames contain normal filename such as cbbc-YYMMDD-HH.ismr
    filenames_backup contain chuc14113s.ismr
    """
    unzipfiles = []
    for filename, filename_backup in zip(filenames, filenames_backup):
        # Extract date and hour from filename
        # Filename format: 'cbbc-YYMMDD-HH.ismr'
        date_str = filename[5:11]  # Extracts YYMMDD
        hour_str = filename[12:14] # Extracts HH
       
        # Convert date string into a datetime object
        date = datetime.strptime(date_str, "%y%m%d")

        # Format year and Julian day
        year_str = date.strftime("%Y")
        julian_day_str = date.strftime("%j")

        # Construct the full URL
        try:
            url = os.path.join(CHAIN_BASE_URL, year_str, julian_day_str, hour_str, f"{filename}.gz")
            zipfile = download(url)
            if zipfile:
                unzip_gz_file(zipfile, zipfile.replace('.gz',''))
                unzipfiles.append(zipfile.replace('.gz',''))
            else:
                url = os.path.join(CHAIN_BASE_URL, year_str, julian_day_str, hour_str, f"{filename_backup}.gz")
                zipfile = download(url)
                if zipfile:
                    unzip_gz_file(zipfile, zipfile.replace('.gz',''))
                    unzipfiles.append(zipfile.replace('.gz',''))
                else:
                    raise Exception(f"{filename} and {filename_backup} CANNOT BE DOWNLOADED!")
                 
        except Exception as e:
            savepath = os.path.join(CHAIN_FOLDER, filename)
            with open(savepath, 'wb') as file:         
                # Write the content to the file in chunks
                pass
            


    return unzipfiles


def download(url):
    """download data
    """
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            savepath = os.path.join(CHAIN_FOLDER, url.split('/')[-1])
            # Open a local file in binary write mode
            with open(savepath, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    # Write the content to the file in chunks
                    file.write(chunk)
            print(f"{url.split('/')[-1]} DOWNLOADED!")
            return file.name
    except Exception as e:
        return []



def unzip_gz_file(input_file, output_file):
    """
    Unzips a .gz file and saves the decompressed content to the output file.
    """
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    print(f"File decompressed: {output_file}")


def gpstoutc(gpsWeek, gpsTow):
    # Constants
    gpsEpoch = dateutil.parser.parse('1980-01-06 00:00:00') # GPS epoch start
    leapSeconds = 17.0  # Difference between GPS time and UTC as of 2014
    
    # Calculate the number of days since the GPS epoch
    daysSinceGpsEpoch = gpsWeek * 7 + gpsTow / 86400.0

    # Calculate the UTC time
    utcTime = gpsEpoch + timedelta(days=daysSinceGpsEpoch) - timedelta(seconds=leapSeconds)

    return utcTime