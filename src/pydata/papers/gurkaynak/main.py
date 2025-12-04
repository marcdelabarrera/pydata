import pandas as pd


def read_data(date_from:pd.Timestamp = None, date_to:pd.Timestamp = None) -> pd.DataFrame:
    df = pd.read_csv("feds200628.csv", skiprows = 8)
    df = df.rename(columns={"Date":"date"})
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    if date_from:
        df = df[date_from:]
    if date_to:
        df = df[:date_to]
    return df