import pandas as pd
from pathlib import Path

PATH = Path(__file__).parent

def recode_state(states:pd.Series)->pd.Series:
    
    STATE_CODES = pd.read_csv(PATH / '../codes/state_codes/state_codes.csv',
                            sep=';',usecols=['state_name','state'])
    STATE_CODES = {i.state_name:i.state for _,i in STATE_CODES.iterrows()}
    return  states.replace(STATE_CODES)

def year_quarter_to_datetime(year:pd.Series,quarter:pd.Series)->pd.Series:
    return pd.to_datetime(year.astype(str) + 'Q'+quarter.astype(str))

def long_to_wide(data:pd.DataFrame)->pd.DataFrame:
    '''
    Data has format series_id, date as index and value as columns.
    '''
    data_wide = data.reset_index().pivot(index='date', columns='series_id',values='value')
    data_wide.columns.name = None
    return data_wide