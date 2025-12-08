from pathlib import Path
import pandas as pd
import numpy as np


PATH = Path(__file__).parent


def _zero_coupon_columns():
    return [f"SVENY{t:02d}" for t in range(1, 31)]

def read_data(date_from:pd.Timestamp = None, date_to:pd.Timestamp = None, columns = None) -> pd.DataFrame:
    df = pd.read_csv(PATH / "feds200628.csv", skiprows = 8)
    df = df.rename(columns={"Date":"date"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    if date_from:
        df = df[date_from:]
    if date_to:
        df = df[:date_to]
    if columns == "zero-cupon":
        df = df[_zero_coupon_columns()]
    return df

def yield_curve(date:pd.Timestamp, series="zero-coupon") -> pd.Series:
    date = pd.to_datetime(date) if isinstance(date, str) else date
    df = read_data()
    date = date if date in df.index else df.index[np.argmin(np.abs(df.index - date))]
    
    if series == "zero-coupon":
        yield_curve = df.loc[date, _zero_coupon_columns()]
    else:
        raise ValueError(f"Unknown series: {series}")
    yield_curve = yield_curve.rename("yield").reset_index(drop=True)
    yield_curve.index = range(1, 30+1)
    yield_curve.index.name = "t"
    return yield_curve