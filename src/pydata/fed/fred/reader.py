
import requests
import pandas as pd
import numpy as np
import time
import datetime 
import os

API_URL = 'https://api.stlouisfed.org/fred'

ListLike = list[str]|pd.Series|str

def set_api_key(api_key:str):
        os.environ['FRED_API_KEY'] = api_key

def _get_api_key()->str:
        api_key = os.environ.get('FRED_API_KEY')
        if api_key is None:
            raise ValueError('API key not set. Please set it using set_api_key().')
        return api_key


def parameters_to_url(parameters:dict)->str: 
        return '&'.join([f'{p}={v}' for p,v in parameters.items()])

def _timestamp_to_str(date:pd.Timestamp)->str:
        return date.strftime('%Y-%m-%d')

def get_series(series_id: ListLike,
               date_from:str, date_to:str = datetime.date.today(),
               api_key:str=None)->pd.DataFrame:
        '''
        Parameters
        series_id: list of series_id
        Returns
        -------
                pd.DataFrame   
                Dataframe with series_id and date as index.
        '''
        api_key = _get_api_key() if api_key is None else api_key
        date_from = _timestamp_to_str(date_from) if isinstance(date_from, pd.Timestamp) else date_from
        date_to = _timestamp_to_str(date_to) if isinstance(date_to, pd.Timestamp) else date_to
        series_id = series_id.to_list() if isinstance(series_id, pd.Series) else series_id
        series_id = [series_id] if isinstance(series_id, str) else series_id
        return pd.concat([_get_series(i, observation_start = date_from, observation_end = date_to, api_key=api_key) for i in series_id])

def _get_series(series_id:str, observation_start:str, observation_end:str, api_key:str=None, cooldown:int=30)->pd.DataFrame:

        parameters = {'series_id': series_id,
                        'api_key': api_key,
                        'observation_start': observation_start,
                        'observation_end': observation_end,
                        'file_type':'json'}
        parameters = parameters_to_url(parameters)
        request = requests.get(f'{API_URL}/series/observations?{parameters}')
        if request.status_code == 200:
                request = request.json()
        else:
                raise ValueError(f'Error in request {request}')
        
        try:
                obs = request['observations'] 
        except KeyError:               
                if  request['error_code']==429:
                        print(f'Error 429: too many requests, waiting {cooldown} seconds')
                        time.sleep(cooldown)
                        return get_series(series_id, observation_start=observation_start, observation_end=observation_end)
                else:
                        raise ValueError(f'Error in request {requests}')
                
        out = pd.DataFrame([[iobs['date'],series_id, iobs['value']] for iobs in obs],
                            columns=['date','series_id','value'])
        out.value = pd.to_numeric(out.value, errors='coerce')
        out.date = pd.to_datetime(out.date, format = '%Y-%m-%d')
        out['src']='fred'
        out = out.set_index(['series_id','date'])
        return out


def _get_series_info(series_id:str, api_key:str)->pd.DataFrame:
        parameters = {'series_id': series_id, 
                        'api_key': api_key,
                        'file_type':'json'}
        parameters = parameters_to_url(parameters)
        request = requests.get(f'{API_URL}/series?{parameters}').json()
        if 'seriess' in request:
                result = pd.DataFrame(request['seriess'])
        elif request['error_code']==429:
                print('Error 429: Too many requests, waiting 30 seconds')
                time.sleep(30)
                return _get_series_info(series_id)
        result = result.rename(columns = {'id':'series_id'})
        return result

def get_series_info(series_id:list[str], api_key:str=None)->pd.DataFrame:
        api_key = _get_api_key() if api_key is None else api_key
        series_id = [series_id] if isinstance(series_id, str) else series_id
        series_id = series_id.to_list() if isinstance(series_id, pd.Series) else series_id
        return pd.concat([_get_series_info(i, api_key=api_key) for i in series_id])

def add_labels(data:pd.DataFrame, labels:list[str] = None)->pd.DataFrame:
        result = pd.merge(data.reset_index(),get_series_info(data.index.get_level_values(0).unique()))
        result = result.set_index(['series_id','date'])
        if labels is not None:
                result = result[['value']+labels]
        return result


def search_by_tag(tag_names:list[str], api_key:str = None)->pd.DataFrame:
        '''
        Finds all series that contain all the tag_names
        tag_names: list of tags
        '''
        api_key = _get_api_key() if api_key is None else api_key
        tag_names = [tag_names] if isinstance(tag_names, str) else tag_names
        parameters = {'tag_names': ';'.join(tag_names), 
                    'api_key': api_key,
                    'file_type':'json'}
        parameters = parameters_to_url(parameters)
        series = requests.get(f'{API_URL}/tags/series?{parameters}').json()['seriess']
        series = pd.DataFrame([[i['id'],i['title'], i['frequency']]for i in series], columns=['series_id','title','frequency'])
        return series

def get_tags(series_id:str, api_key:str = None)->pd.DataFrame:
        '''
        Gets tags from a given series id
        '''
        api_key = _get_api_key() if api_key is None else api_key
        parameters = {'series_id': series_id, 
                      'api_key': api_key,
                      'file_type':'json'}
        parameters = parameters_to_url(parameters)
        tags = requests.get(f'{API_URL}/series/tags?{parameters}').json()['tags']
        tags = pd.DataFrame([[i['name'],i['group_id'], i['notes']]for i in tags], columns=['name','group_id','notes'])
        return tags
