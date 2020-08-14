import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
import Logger 

@Logger.log(name='Drop Unimportant Values')
def drop_unimportant_values(self):
    self.__raw_df__.replace(['',np.nan,'nan'],np.nan,inplace=True)
    self.__raw_df__.replace(regex = [r'\d{1,2}\.\d{1,2}\.\d{2,4}',r'\bAnnexure\b',r'\bToday\b'],value = np.nan,inplace=True)
    self.__raw_df__.dropna(how='all',axis=1,inplace=True)
    self.__raw_df__.dropna(how='all',inplace=True)
    self.__raw_df__.reset_index(drop=True,inplace=True)
    
@Logger.log(name='Clean Values')
def clean_values(df,text_columns = None):
    df.replace(regex=[r'\n',r'[pP]\s*-\s*',r'&'],value=['','',','],inplace=True)
    df = df.apply(lambda x: x.astype(str).str.lower().str.strip())

@Logger.log(name='Reset Columns',df=True)
def reset_columns(self):
    actual_columns = self.__raw_df__.columns 
    columns_to_be = list(self.__raw_df__.iloc[0])
    columns = list(zip(actual_columns,columns_to_be))
    columns = [x if isinstance(x,str) else y for x,y in columns]
    columns = [re.sub(r'\s+',' ',name) for name in columns]
    self.__raw_df__.columns = columns
    self.__raw_df__.drop(index=0,inplace=True)
    self.__raw_df__ = self.__raw_df__.reset_index(drop=True)
    

