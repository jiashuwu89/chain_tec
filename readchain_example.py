
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
import requests
import gzip
import shutil

THEMIS_FOLDER = "themis"
THEMIS_COLUMN = ["th_id", "min_distance_time", "orbit_start_time", "orbit_end_time", 
                 "minDistance", "min_distance_lat", "min_distance_lon", "start_lat", 
                 "start_lon", "end_lat", "end_lon"]
FONTSIZE = 12
FIGSIZE = (10, 8)

CHAIN_FOLDER = "chain"
CHAIN_BASE_URL = "http://chain.physics.unb.ca/data/gps/ismr/"

DOWNLOAD_FLG = True

OUTPUT_FOLDER = "figure"
def read_chain(filenames):
    """
    Reads and combines data from a list of files.
    Assumes a specific data format as in the original MATLAB function.
    """
    all_data = None

    # Loop through each file
    for filename in filenames:
        try:
            # Read the data assuming a CSV format with a header
            data = pd.read_csv(os.path.join(CHAIN_FOLDER, filename), header=0, delimiter=',')

            # If this is the first file, initialize all_data with its contents
            if all_data is None:
                all_data = data
            else:
                # Otherwise, append the data from this file to all_data
                all_data = pd.concat([all_data, data], ignore_index=True)
        except FileNotFoundError:
            print(f"File not found: {filename}, skip reading this file.")
            continue
        except Exception as e:
            print(f"Error reading file {filename}: {e}")
            continue

    return all_data


def read_themis(filename) -> pd.DataFrame:
    """
    Reads themis info from txt file
    """
    themis_data = pd.read_csv(os.path.join(THEMIS_FOLDER, filename), header=None, names=THEMIS_COLUMN, delimiter='\s+')
    # add delimiter is because the space in the file is not consistent

    return themis_data


def plot_chain(time_min, prns, ids, elevations, s4, sigma_phi, stec, l1_cno, l2_cno, outputname):
    """
    Make plot of chain data and save
    """
    for prn in ids:
        ind_select = prns == prn

        plt.figure(figsize=FIGSIZE)

        ax1 = plt.subplot(5, 1, 1)
        ax1.plot(time_min[ind_select], elevations[ind_select], 'k-', linewidth=2)
        ax1.set_xlim([0, 80])
        ax1.set_ylabel('Elevation [deg]', fontsize=FONTSIZE)
        ax1.tick_params(axis='both', which='major', labelsize=FONTSIZE)
        ax1.set_title(outputname, fontsize=FONTSIZE+5)

        ax2 = plt.subplot(5, 1, 2)
        ax2.plot(time_min[ind_select], s4[ind_select], 'k-', linewidth=2)
        ax2.set_xlim([0, 80])
        ax2.tick_params(axis='both', which='major', labelsize=FONTSIZE)
        ax2.set_ylabel('S4 index', fontsize=FONTSIZE)

        ax3 = plt.subplot(5, 1, 3)
        ax3.plot(time_min[ind_select], sigma_phi[ind_select], 'k-', linewidth=2)
        ax3.set_xlim([0, 80])
        ax3.set_ylabel('sigma [rad]', fontsize=FONTSIZE)
        ax3.tick_params(axis='both', which='major', labelsize=FONTSIZE)

        ax4 = plt.subplot(5, 1, 4)
        ax4.plot(time_min[ind_select], stec[ind_select], 'k-', linewidth=2)
        ax4.set_xlim([0, 80])
        ax4.set_ylabel('TECu', fontsize=FONTSIZE)
        ax4.tick_params(axis='both', which='major', labelsize=FONTSIZE)

        ax5 = plt.subplot(5, 1, 5)
        ax5.plot(time_min[ind_select], l1_cno[ind_select], 'k-', linewidth=2, label="L1_cno")
        ax5.plot(time_min[ind_select], l2_cno[ind_select], 'r-', linewidth=2, label="L2_cno")
        ax5.set_xlim([0, 80])
        ax5.set_ylabel('L1/L2 CNo', fontsize=FONTSIZE)
        ax5.tick_params(axis='both', which='major', labelsize=FONTSIZE)
        ax5.legend()

        plt.savefig(f"{outputname}_PRN{prn}.png")
        plt.close()


def themis_chain_map(start_time: str, end_time: str, station: str):
    """
    Parse themis start and end time and generate the list of 
    filenames to download from chain, and generate a output filename for figure
    """
    start_time_dt = datetime.strptime(start_time, '%Y-%m-%d/%H:%M:%S')
    end_time_dt = datetime.strptime(end_time, '%Y-%m-%d/%H:%M:%S')

    filenames = []
    current_time = start_time_dt
    while current_time <= end_time_dt:
        filename = current_time.strftime('cbbc-%y%m%d-%H.ismr')
        filenames.append(filename)
        current_time +=  timedelta(hours=1)

    # get outputname 
    start_time_str = start_time.replace('-','').replace('/','_').replace(':','')
    end_time_str = end_time.replace('-','').replace('/','_').replace(':','')
    outputname = station+'_'+start_time_str+end_time_str[8:]

    # get deltaT
    previous_hour = start_time_dt.replace(minute=0, second=0, microsecond=0)
    deltaT = (start_time_dt - previous_hour).total_seconds()

    return filenames, outputname, deltaT


def chain_main(filenames: str, outputname: str, deltaT: float):
    """Load chain data and plot
    """

    # Read and combine the data
    combined_data = read_chain(filenames)

    # Accessing combined data
    # Assuming specific columns exist in the data
    weeks = combined_data['Week']
    times = combined_data[' GPS TOW ']
    prns = combined_data['PRN']
    elevations = combined_data['  Elv ']
    s4 = combined_data[' S4 ']
    sigma_phi = combined_data['60SecSigma']
    stec = combined_data[' TEC0']
    l1_cno = combined_data['L1 CNo']
    l2_cno = combined_data[' L2 CNo']

    # Filter based on elevation and get unique PRNs
    select = elevations > 50.0
    ids = prns[select].unique()

    # Convert times to minutes from a specific start time
    time0 = times.iloc[0] + deltaT + 30.0
    time_min = (times - time0) / 60.0

    # Plotting
    plot_chain(
        time_min, prns, ids, elevations, s4, sigma_phi, stec, l1_cno, l2_cno, 
        os.path.join(OUTPUT_FOLDER, outputname))
        

def chain_download(filenames: list[str]):
    """
    Download chain data from http://chain.physics.unb.ca/data/gps/ismr/. 
    construct url with filnames
    """
    for filename in filenames:
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
        url = os.path.join(CHAIN_BASE_URL, year_str, julian_day_str, hour_str, f"{filename}.gz")
        zipfile = download(url)
        if zipfile:
            unzip_gz_file(zipfile, zipfile.replace('.gz',''))

    return

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
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"{url.split('/')[-1]} CANNOT BE DOWNLOADED!")
    except Exception as e:
        print(f"Error: {e}")
        print(f"{url.split('/')[-1]} CANNOT BE DOWNLOADED!")

    return


def unzip_gz_file(input_file, output_file):
    """
    Unzips a .gz file and saves the decompressed content to the output file.
    """
    with gzip.open(input_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    print(f"File decompressed: {output_file}")


station = "cbb"
themis_name = station+"_THEMIS_CHAIN.txt"
themis_data = read_themis(themis_name)

for start_time, end_time in zip(themis_data['orbit_start_time'], themis_data['orbit_end_time']):
    chain_filenames, outputname, deltaT = themis_chain_map(start_time, end_time, station)
    if DOWNLOAD_FLG == True:
        chain_download(chain_filenames)
    chain_main(chain_filenames, outputname, deltaT)
   