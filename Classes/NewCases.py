import os 
import re
import pandas as pd
import numpy as np
import Logger 
import time

from Document import Document
import Row as rows
import Table as table
from Column import Columns 

pd.options.display.max_rows = 200

class NewCases(): 

    @Logger.log(name='WRAPPER',wrapper=True,df=True)
    def __wrapper__(self,func,*args,**kwargs):
        logger_message = func(*args,**kwargs)
        return logger_message
        
    @Logger.log(name='Split Source')
    def __split_description__(self):
        source_col = self.columns.get('SOURCE')
        if source_col is not None:
            to_replace_source = {r'^contact.*':'contact', r'ili':'ili',r'sari':'sari',r'^(returnee|international).*':'returnee',r'^(inter|district|travel\s+history).*':'idt'}
            to_replace_meta = {r'^contact\s+(?:of|under)?\s*(?:[pP])?\s*[-–]?\s*(\w+\s*\w+|\w+|\d+)$':r'\1',r'^(returnee|international).* (from)? (.*)':r'\3',r'^(inter|district).*[-–,]\s*?(\w+|\w+-\w+|\w+\s+\w+)$':r'\2'}
            source = self.__raw_df__[source_col]
            self.__raw_df__['source'] = source.replace(to_replace_source,regex=True) 
            self.__raw_df__['meta'] = source.replace(to_replace_meta,regex=True).replace('tracing','tbd') 

    @Logger.log(name='Format Sex')
    def __format_sex__(self):
        sex_col = self.columns.get('SEX')
        if sex_col is not None:
            self.__raw_df__[sex_col].replace({r'^[Ff].*':'f',r'^[Mm].*':'m'},regex=True,inplace=True)

    @Logger.log(name='Split Isolation Location')
    def __split_isolation_location__(self):
        isoat_col = self.columns.get('ISOAT')
        if isoat_col is not None:
            to_replace_isolated_location = {r'^(?:brought|died)(?:.*)((?:\b\w+\b)\s*hospital|residence)(?:.*)':r'deceased - \1',self.columns.COLUMNS['ISOAT']['REGEX']:r'\1'}
            to_replace_isolated_district = {r'^(?:brought|died|designated|private).*(?:(?<=(?:her|his))|(?<=[,])|(?<=hospital))(.*)$':r'\1'}
            isolated_at = self.__raw_df__[isoat_col]
            self.__raw_df__['isolated in'] = isolated_at.replace(to_replace_isolated_location,regex=True)
            self.__raw_df__['isolated district'] = isolated_at.replace(to_replace_isolated_district,regex=True,)

    @Logger.log(name='Add Positive Result Date')
    def __add_positive_result_date__(self):
        self.__raw_df__['+ve result date'] = self.date

    @Logger.log(name='Remove Redundant Columns')
    def __remove_redundant_cols__(self):
        cols = self.columns.get()
        self.__raw_df__.drop(columns=[cols['DPNO'],cols['SNO'],cols['ISOAT'],cols['SOURCE']],inplace=True,errors='ignore')
        self.__raw_df__.reset_index(drop=True,inplace=True)

    @Logger.log(name='Clean Values and Strip strings')
    def __clean_values__(self,replace_pno=False):
        to_replace = {r'\n':'',r'&':','} if not replace_pno else {r'[pP]\s*-\s*':''}
        self.__raw_df__.replace(to_replace,regex=True,inplace=True)        
        self.__raw_df__ = self.__raw_df__.apply(lambda x: x.astype(str).str.lower().str.strip())
    
    def __process__(self): 
        table.drop_unimportant_values(self)
        table.reset_columns(self,columns_determined=False) 
        self.__clean_values__()
        self.columns.determine(self.__raw_df__)
        self.__clean_values__(True)
        rows.join(self)
        self.__split_description__()
        self.__split_isolation_location__()
        self.__add_positive_result_date__()
        self.__remove_redundant_cols__()
        self.__format_sex__()
        self.columns.format_age(self.__raw_df__)
        table.checks(self)

    def get(self):
        return self.__raw_df__
    
    def __init__(self,doc):
        table.IS = 'NEWCASES'
        print('\n\nNew cases parsing started')
        self.date = doc.filename

        Logger.init(f'/data/logs/newcases/{self.date}')

        if not doc.exists('NEWCASES'):
            Logger.message(f'New Cases table doesn\'t exist or is not readable')
            return None 

        start = time.perf_counter() 
        self.__raw_df__ = doc.get_tables('NEW')
        Logger.message(f'Took {time.perf_counter() - start}s to load New Cases')

        self.columns = Columns(['PNO','SNO','DPNO','ISOAT','SOURCE','SEX','DISTRICT','AGE'])
        self.columns.set_frequencies(unit=['SNO','PNO','DPNO'],multiple=['SOURCE','ISOAT','AGE'])

        start = time.perf_counter()
        self.__process__()
        Logger.message(f'Took {time.perf_counter() - start}s to process New Cases')
        Logger.drop()
        print('New cases parsing completed')
    
def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def main():
    file_name = '03-07-2020'
    Logger.init(f'/data/logs/newcases/{file_name}')
    pdf_path = create_abs_path(f'/data/06-07/{file_name}.pdf')
    doc = Document(pdf_path,file_name)
    newcases = NewCases(doc)
    df = newcases.get()
    print(df)

if __name__ == "__main__":
    main()