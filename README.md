# Column names and column types
* `date`: date (pd.datetime)
* `state`: 2 letter abreviation (str) (national is US)
* `state_code`: digit code (str) (national is 00)
* `state_name`: full name (str)
* `country`: 3 letter abreviation (str)
* `country_name`: full name (str)
* `industry_code`: digit code (str)
* `industry_name`: full name (str)
* `naics_yy`: naics of a given year (int)
* `naicsd`: naics with d digits (int)
* `naics`: generic naics (int)
* `naics_title`: description of naics (str)
* `fips`: 5 digit fips code (`state_code+county_code`) (int)
* `county_code`: 3 digit county code (str)
* `county_name`: county name (str)
* `msa_name`: metropolitan statistical area name, state included (str)
* `msa_code`: metropolitan statistical area name code (str)
* `cbsa_code`: core based statistical area, combines Metropolitan Statistical Areas and Micropolitan Statistical Areas (str)



|column_name|dtype|description|
|-|-|-|
|`date`|`pd.datetime`|
|`year`|`Int16`|
|`state`|`str`|Two letter string
|`state_code`|`Int8`|
|`state_name`|`str`|
|`msa_code`|`Int32`|
|`cbsa_code`|`Int32`|
|`fips`|`Int32`|`state_code`+`county_code`|
|`county_code`|`Int16`|3 digit county code





# Crosswalks

There are some discrepancies in msa_codes. Use `crosswalks/county_cbsa_code/cbsa_codes.parquet`.


### Structure of the folders
The first folder is the source (BLS, NBER, BEA...).
If a given source has several datasets, have one folder per dataset.
Each folder should have a `raw_data` folder and a `data` folder. `raw_data` can be empty 
if data is directly queried using an API or the dataset is too big. 

```
data
|-source 
    |-dataset 
        |-code
            |-main.py
            |-test.ipynb
            |-playground.ipynb
        |-meta
            |-dataset_codes.csv
        |-raw_data
        |-docs
            |-links
            |-documentation.pdf
        |-dataset.csv
        |-dataset.pq
```

### Date convention
For flows like GDP, or investment, '2000-01-01' is 2000, 2000Q1, or Jan-2000. 

For stock like unemployment, '2000-01-31' indicates the January unemployment.

### Naming
startyear
endyear
### Format 
The standard format to save files is in `csv` with separator `;`. Save the index
only when it is a datetime. Long format is encouraged.
Index as datetime with name `date`, `series_id` and `value` and `scr` `units` and then extra columns.
This allows to concat several datasets from different sources.
```
data.to_csv('filename.csv',sep=';',index=False)
data = pd.read_csv('filename.csv', sep=';')
```
For if index is a datetime.
```
data.to_csv('filename.csv',sep=';')
data = pd.read_csv('filename.csv', sep=';', index_col='date', parse_dates=True)
```

### Naics
naics_07. _07 indicates the year
naics4 is 4 digit naics


### Code

```
from pathlib import Path
import logging
import pandas as pd

PATH = Path('//bbking2.mit.edu/mbarrera/git_supply/data/new_source/new_dataset')

logging.basicConfig(filename=PATH / f'code/logs/{Path(__file__).stem}.log', 
                    format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.INFO,
                    filemode='w',
                    datefmt='%Y-%m-%d %H:%M:%S')

logging.info('Process completed')
```