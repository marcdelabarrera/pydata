# Standard imports
from pathlib import Path
import tempfile
import shutil

# Third party imports
from ipumspy import IpumsApiClient, MicrodataExtract, readers
import pandas as pd
import os

# Samples id https://cps.ipums.org/cps-action/samples/sample_ids

def set_api_key(api_key:str):
    os.environ['IPUMS_API_KEY'] = api_key

def _get_api_key()->str:
        api_key = os.environ.get('IPUMS_API_KEY')
        if api_key is None:
            raise ValueError('API key not set. Please set it using set_api_key().')
        return api_key


def read_cps_extract(extract: MicrodataExtract, download_dir = None, api_key = None)->pd.DataFrame:
    print(f'Reading cps extract: {extract}')

    api_key = _get_api_key() if api_key is None else api_key
    delete_dir = False
    if download_dir is None:
        download_dir = tempfile.mkdtemp()
        delete_dir = True
    ipums = IpumsApiClient(API_KEY)
    ipums.submit_extract(extract)
    ipums.wait_for_extract(extract)
    ipums.download_extract(extract, download_dir=download_dir)
    print(f'Extract downloaded')
    extract_file = f"{extract.collection}_{str(extract.extract_id).zfill(5)}"
    renae_ddi = readers.read_ipums_ddi(f'{download_dir}/{extract_file}.xml')
    cps_data = readers.read_microdata(renae_ddi, f'{download_dir}/{extract_file}.dat.gz')
    if delete_dir:
        shutil.rmtree(download_dir)
    cps_data.columns = cps_data.columns.str.lower()
    #cps_data = add_date_column(cps_data)

    return cps_data

def add_date_column(cps_data:pd.DataFrame)->pd.DataFrame:
    '''
    Add a date column to the cps data and remove the year and month columns
    '''
    if 'year' not in cps_data or 'month' not in cps_data:
        raise ValueError('Data must contain columns "year" and "month"')
    cps_data['date'] = pd.to_datetime(cps_data['year'].astype(str)+cps_data['month'].astype(str), format='%Y%m')
    cps_data = cps_data.drop(columns = ['year','month'])
    return cps_data


def extract_to_csv(extract: MicrodataExtract,
                   filename: Path)->None:
    '''
    Given a cps extract, download the extract and save it as a csv file
    '''
    data = read_cps_extract(extract)
    data.to_csv(filename, sep=';',index=False)
    

#cps_samples = pd.read_csv(Path(__file__).parent / 'cps_samples.csv',sep=';')

