import pandas as pd
import os
import requests
import gzip
import shutil
from datetime import datetime, timedelta
import dateutil.parser
import parameter
import logging

# Configure logging
log_filename = 'chain_download.log'
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename=log_filename,
                    filemode='a')  # 'a' means append; use 'w' to overwrite each time

def chain_download(filenames: list[str]):
    """
    Download chain data from https://www.chain-project.net/data/gps/data/raw/
    construct url with filnames
    """
    unzipfiles = []
    for filename in filenames:
        # Extract year, jday, hour from filename
        jday_str = filename[4:7]  
        year_str = '20'+filename[9:11]
        hour_str = filename[7] # Extracts HH
        station = filename[:4]

        # Convert julian date to month
        start_of_year = datetime(int(year_str), 1, 1)
        date = start_of_year + timedelta(days=int(jday_str) - 1)  # Subtract 1 because January 1st is Julian day 1
  
        # Check whether this file exist alread
        if check_exist(filename):
            logging.info(f"{filename} ALREADY DOWNLOADED, SKIP!")
            continue

        # Construct the full URL
        try:
            url = os.path.join(parameter.CHAIN_BASE_URL, station, year_str, f"{date.month:02d}", filename)
            zipfile = download(url)
            if zipfile:
                try:
                    unzip_gz_file(zipfile, zipfile.replace('.gz',''))
                    unzipfiles.append(zipfile.replace('.gz',''))
                except:
                    logging.error(f"{filename} CANNOT BE UNZIP!")
                    breakpoint()
                 
        except Exception as e:
            logging.error(f"{filename} CANNOT BE DOWNLOADED!")
            breakpoint()
        
    return unzipfiles


def check_exist(filename):
    """checkt whether file already exist in the download folder
    """
    file_path = os.path.join(parameter.CHAIN_FOLDER, filename)
    return os.path.exists(file_path)

def download(url):
    """download data
    """
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            savepath = os.path.join(parameter.CHAIN_FOLDER, url.split('/')[-1])
            # Open a local file in binary write mode
            with open(savepath, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    # Write the content to the file in chunks
                    file.write(chunk)
            logging.info(f"{url.split('/')[-1]} DOWNLOADED!")
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
