import pandas as pd
import numpy as np




def read_data(columns:list=None, date_from:pd.Timestamp=None, date_to:pd.Timestamp=None)->pd.DataFrame:
    df = pd.read_excel("https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab25-4763ac982e53/downloads/2d6fa720-95d6-4953-b869-1948ad39173e/ie_data.xls?ver=1764695321121",
                        sheet_name="Data", skiprows=7, skipfooter=1)
    df = df.rename(columns={"Date":"date",
                        "P":"spy_price",
                        "D":"spy_dividend",
                        "E":"spy_earnings",
                        "CPI":"cpi",
                        "Rate GS10": "long_interest_rate",
                        "Price": "real_spy_price",
                        "Dividend":"real_spy_dividend",
                        "Price.1": "real_total_spy_returns",
                        "Earnings":"real_spy_earnings",
                        "Returns":"monthly_total_bond_returns",
                        "Returns.1": "real_total_bond_returns"})
    df["date"] = pd.to_datetime(df["date"].astype("str").str.replace(r"\.1$",".10",regex=True), format="%Y.%m")
    df = df.set_index("date")

    columns = [i for i in columns if i != "date"] if columns else None
    
    if columns:
        df = df[columns]
    if date_from:
        df = df.loc[date_from:]
    if date_to:
        df = df.loc[:date_to]
    return df