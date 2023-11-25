
import pandas as pd
from datetime import datetime, timedelta
import os

from chain_data import *

THEMIS_FOLDER = "themis"
THEMIS_FILE = "themis_chain_2014_updated.csv"
DOWNLOAD_FLG = True


def read_themis(filename) -> pd.DataFrame:
    """
    Reads themis info from csv
    :param filaname
    :return
    """
    try:
        return pd.read_csv(os.path.join(THEMIS_FOLDER, filename))
    except FileNotFoundError:
        print(f"File not found: {filename}")

    return


def filter_themis(themis_data:pd.DataFrame, quality=None, station=None):
    """
    Select events from themis dataframe
    """
    if quality is not None:
        themis_data = themis_data[themis_data['quality'] == quality]
    if station is not None:
        themis_data = themis_data[themis_data['station'] == station]

    return themis_data[['station','themis_id','start_time','end_time','quality']]


def themis_chain_map(start_time: str, end_time: str, station: str, themis_id: str):
    """
    Parse themis start and end time and generate the list of 
    filenames to download from chain, and generate a output filename for figure
    """
    start_time_dt = datetime.strptime(start_time, '%Y-%m-%d/%H:%M:%S')
    end_time_dt = datetime.strptime(end_time, '%Y-%m-%d/%H:%M:%S')

    filenames = []
    current_time = start_time_dt
    filenames_backup = []
    while current_time <= end_time_dt:
        filename = current_time.strftime(f'{station}c-%y%m%d-%H.ismr')
        filenames.append(filename)
        current_time +=  timedelta(hours=1)
        hour_str = chr(current_time.hour + 97 - 1)
        julian_day_str = current_time.strftime("%y%j")
        filenames_backup.append(f"{station}c{julian_day_str}{hour_str}.ismr")

    # get outputname 
    start_time_str = start_time.replace('-','').replace('/','_').replace(':','')
    end_time_str = end_time.replace('-','').replace('/','_').replace(':','')
    outputname = start_time_str+end_time_str[8:]+'_'+station+'_'+themis_id
    
    return filenames, outputname, filenames_backup


if __name__ == "__main__":
    themis_data = read_themis(THEMIS_FILE)
    themis_data_filter = filter_themis(themis_data, quality=2)

    for index, row in themis_data_filter.iterrows():
        chain_filenames, outputname, chain_filenames_backup = themis_chain_map(row['start_time'], row['end_time'], row['station'], row['themis_id'])
        if DOWNLOAD_FLG == True:
            chain_download(chain_filenames, chain_filenames_backup)
        chain_main(chain_filenames, chain_filenames_backup, outputname, starttime=row['start_time'], endtime=row['end_time'])
   