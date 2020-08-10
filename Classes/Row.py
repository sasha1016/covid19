import pandas as pd
import numpy as np

def row_mostly_null(row):
    counts = row.value_counts(dropna=False)
    most_frequent_value = counts.index[0]
    if (most_frequent_value == 'nan' or pd.isna(most_frequent_value)) and counts.to_list()[0] != 1 :
        return True
    else:
        return False

class Rows():

    def join(self,df,delimiters=None):
        disjoint_rows = [index for index,row in df.iterrows() if row_mostly_null(row)]
        try:
            for row in disjoint_rows:
                df.loc[row] = df.loc[row].replace({np.nan:'','nan':''})
                df.loc[row-1] = df.loc[row-1].apply(str) + ' ' + df.loc[row].apply(str)
                if delimiters is not None:
                    for column in delimiters:
                        if df.at[row,column] != '':
                            df.at[row-1,column] = (df.at[row-1,column] + delimiters[column] + df.at[row,column])
                df.loc[row] = np.nan
        except Exception as e:
            print(e)
        df = df.dropna(how='all',axis=0)
        return df

    def __init__(self):
        print('asd')


def join(df,delimiters=None):
    name = 'Row Join'
    disjoint_rows = [index for index,row in df.iterrows() if row_mostly_null(row)]
    for row in disjoint_rows:
        df.loc[row] = df.loc[row].replace({np.nan:'','nan':''})
        df.loc[row-1] = df.loc[row-1].apply(str) + ' ' + df.loc[row].apply(str)
        if delimiters is not None:
            for column in delimiters:
                if df.at[row,column] != '':
                    df.at[row-1,column] = (df.at[row-1,column] + delimiters[column] + df.at[row,column])
        df.loc[row] = np.nan
    df.dropna(how='all',axis=0,inplace=True)
    return (name,'joined all disjoint rows',disjoint_rows)
        