import pandas as pd

def mergeTables(df1 : pd.DataFrame, df2 : pd.DataFrame) -> pd.DataFrame:
    """
    Merges two dataframes into one
    """
    result = pd.merge(df1, df2)
    return result