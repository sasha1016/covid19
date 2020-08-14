import os 
import re
import pandas as pd
import numpy as np
from dateutil import parser
from datetime import datetime
import time

from Document import Document
from Column import Columns 
import Table as table 
import Row as rows

import Logger

pd.options.display.max_rows = 200

class Deaths():

    __raw_df__ = None 

    @Logger.log(name='Add Dead on Arrival',message="Dead on Arrival column added for all rows",df=True)
    def __add_dead_on_arrival_column__(self):
        regex = re.compile(r'^brought|died|dead.*',re.I)
        column_name = self.columns.get('DOA')
        doa = self.__raw_df__[column_name]
        self.__raw_df__['dead on arrival'] = np.where(doa.str.contains(regex), 1 , 0)

    @Logger.log(name="Format DOA",message="Converted all Date of Admissions",df=True)
    def __format_doa__(self):
        regex = self.columns.COLUMNS['DOA']['REGEX']
        doa = self.columns.get('DOA')
        self.__raw_df__[doa] = self.__raw_df__[doa].str.extract(regex)
        date_components_regex = re.compile(r'(?:brought|died\s*\w+)?(?:(\d{1,2})\s*[-/.]\s*(\w{2,3}|\d{1,2})\s*[-/.]\s*(\d{2,4}))$',re.I)
        self.__raw_df__[doa].replace(to_replace=date_components_regex,value=r'\1-\2-\3',inplace=True)
        self.__raw_df__[doa].replace(to_replace=np.nan,value='nan',inplace=True)
    
    def __join__(self):
        symptoms,comobs = self.columns.get('SYMPTMS'), self.columns.get('CMRBDTS')
        delimiters = {} 
        delimiters[symptoms], delimiters[comobs] = ',', ',' 
        rows.join(self,delimiters) 
    
    @Logger.log(name="Convert to Datetime",message="Converted column values to datetime")
    def __convert_to_datetime__(self,column_name):
        column = self.columns.get(column_name)
        self.__raw_df__[column] = self.__raw_df__[column].apply(lambda x: parser.parse(x,fuzzy=True,dayfirst=True) if x != 'nan' else 'nan')
    
    @Logger.log(name="Add Duration column",message="Duration of Admission column added")
    def __add_duration__(self):
        df = self.__raw_df__
        doa, dod = self.columns.get('DOA'), self.columns.get('DOD')
        df['duration'] = df[dod] - df[doa]
        
    @Logger.log(name="Explode List",message="Exploded all comma seperated values for column")
    def __explode__(self,column_name):
        df = self.__raw_df__
        column = self.columns.get(column_name)
        #df = df.assign(cmrbdty=df[colun].str.split(',')).explode('cmrbdty')
        df[column] = df[column].str.split(',')
        df = df.explode(column)
        self.__raw_df__ = df

    @Logger.log(name='Clean Values and Strip strings')
    def __clean_values__(self,replace_pno=False):
        to_replace = {r'\n':'',r'&':','} if not replace_pno else {r'[pP]\s*-\s*':''}
        self.__raw_df__.replace(to_replace,regex=True,inplace=True)        
        self.__raw_df__.replace('-','',inplace=True)
        self.__raw_df__ = self.__raw_df__.apply(lambda x: x.astype(str).str.lower().str.strip())
    

    def __process__(self):
        table.drop_unimportant_values(self)
        self.__clean_values__()
        table.reset_columns(self) # Resetting columns assuming the table is read perfectly, which is an issue
        self.columns.determine(self.__raw_df__)
        self.__clean_values__(True)
        self.columns.format_values(column='DOA',df = self.__raw_df__)
        self.__join__()
        self.__add_dead_on_arrival_column__()
        self.__format_doa__()
        self.__convert_to_datetime__('DOA')
        self.__convert_to_datetime__('DOD')
        self.__add_duration__()

    def __init__(self,doc):

        Logger.init(f'/data/logs/deaths/{doc.filename}')

        start = time.perf_counter()
        self.__raw_df__ = doc.get_tables('DEATHS',force=True)
        Logger.message(f'Took {time.perf_counter() - start}s to load deaths')

        self.columns = Columns(['SNO','PNO','DISTRICT','AGE','SEX','SOURCE','SYMPTMS','CMRBDTS','DOA','DOD'])
        self.columns.set_frequencies(unit=['SNO','PNO','DOA','DOD'],multiple=['AGE','SOURCE','CMRBDTS','DOA','DOD','SYMPTMS'])

        start = time.perf_counter()
        self.__process__()
        Logger.message(f'Took {time.perf_counter() - start}s to process deaths')
    
    def get(self):
        return self.__raw_df__
    
def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def main():
    file_name = '05-07-2020'
    Logger.init(f'/data/logs/deaths/{file_name}')
    pdf_path = create_abs_path(f'/data/06-07/{file_name}.pdf')
    doc = Document(pdf_path,file_name)
    deaths = Deaths(doc)
    df = deaths.get()
    print(df)

if __name__ == "__main__":
    main()