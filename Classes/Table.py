import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np

def drop_unimportant_values(df):
    name = 'Drop Unimportant Values'
    df.replace(['',np.nan,'nan'],np.nan,inplace=True)
    df.replace(regex = [r'\d\d\.\d\d\.\d\d\d\d',r'\bAnnexure\b',r'\bToday\b'],value = np.nan,inplace=True)
    df.dropna(how='all',axis=1,inplace=True)
    df.dropna(how='all',inplace=True)
    df.reset_index(drop=True,inplace=True)
    return (name,None,None)
    

def clean_values(df,text_columns = None):
    name='Clean Dataframe values'
    df.replace(regex=[r'\n',r'[pP]\s*-\s*',r'&'],value=['','',','],inplace=True)
    df = df.apply(lambda x: x.astype(str).str.lower().str.strip())
    #df.astype(str).apply(lambda x: x.lower().strip())
    # for column in text_columns:
    #     df[column] = df[column].astype(str).apply(lambda x: x.lower().strip())
    return (name,None,None)

def reset_columns(df):
    name='Reset Columns'
    actual_columns = df.columns 
    columns_to_be = list(df.iloc[0])
    columns = list(zip(actual_columns,columns_to_be))
    columns = [x if isinstance(x,str) else y for x,y in columns]
    columns = [re.sub(r'\s+',' ',name) for name in columns]
    df.columns = columns
    df.drop(index=0,inplace=True)
    return (name,None,columns)
    

