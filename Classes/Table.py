import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
import Logger 

IS = None 
COLUMN_COUNTS = {'RECOVERIES':6,'DEATHS':12,'NEWCASES':9}

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
def reset_columns(self,columns_determined=True):
    actual_columns = self.__raw_df__.columns 
    columns_to_be = list(self.__raw_df__.iloc[0])
    columns = list(zip(actual_columns,columns_to_be))

    if columns_determined:
        columns_determined = list(self.columns.get().items())
        columns_to_be_set = {}
        # u = Actual Column ,v = Column to Be, x = Column Name eg [DISTRICT,PNO,SNO etc], y = Column that's been determined eg [1,3,4]
        for u,v in columns:
            for x,y in columns_determined:
                if u == y:
                    columns_to_be_set[x] = v 
        self.columns.set_columns(dict(columns_to_be_set))

    columns = [x if isinstance(x,str) else y for x,y in columns]
    columns = [re.sub(r'\s+',' ',name) for name in columns]
    self.__raw_df__.columns = columns
    self.__raw_df__.drop(index=0,inplace=True)
    self.__raw_df__ = self.__raw_df__.reset_index(drop=True)

def checks(self,*args):
    print('Checking ...')

    def nans_and_nats():
        df = self.__raw_df__.replace(to_replace={'nan':np.nan,'nat':pd.NaT})
        null_values_exist = False 
        for index,row in df.iterrows():
            null_values = row.isna()
            if True in null_values.values:
                null_values_exist = True
                print(f'{null_values.values.tolist().count(True)} null values in row {index}')

        if not null_values_exist:
            print('No null values in the DF')

    def structure():
        cols = self.__raw_df__.shape[1]
        if cols != COLUMN_COUNTS[IS]:
            print('Column count doesn\'t match')
        else:
            print('Column count matches')

    def row_join_error():
        district_col = self.columns.get('DISTRICT')
        districts = self.__raw_df__[district_col] 
        district_values = self.columns.COLUMNS['DISTRICT']['VALUES']
        no_row_join_error = True
        for row,district in districts.items():
            if district not in district_values:
                if district.strip() not in district_values:
                    no_row_join_error = False 
                    print(f'Possible row join error in row {row}, {district}')
        if no_row_join_error:
            print(f'No Row Join Errors in DF')

    nans_and_nats()
    structure()
    row_join_error()

    for check in args:
        check(self)
    
    print('Checks completed!')
    

