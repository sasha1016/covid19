import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
import Logger 

from Document import Document
import Row as rows
import Table as table
from Column import Columns 

pd.options.display.max_rows = 200

file_name = '02-07-2020'
Logger.init(f'/data/logs/{file_name}')

def row_mostly_null(row):
    if row.value_counts().index[0] == 'nan':
        return True
    else:
        return False


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
        source_col = self.columns.get('SEX')
        print(source_col)
        if source_col is not None:
            sex = self.__raw_df__[source_col]
            sex.replace({r'^[Ff]':'f',r'^[Mm]':'m'},inplace=True)

    @Logger.log(name='Split Isolation Location')
    def __split_isolation_location__(self):
        isoat_col = self.columns.get('ISOAT')
        if isoat_col is not None:
            to_replace_isolated_location = {r'^(?:brought|died)(?:.*)((?:\b\w+\b)\s*hospital|residence)(?:.*)':r'deceased - \1',r'^(\w+\s+\w+)[,-]?(.*)':r'\1'}
            to_replace_isolated_district = {r'^(?:brought|died).*(?:(?<=(?:her|his))|(?<=[,])|(?<=hospital))(.*)$':r'\1'}
            isolated_at = self.__raw_df__[isoat_col]
            self.__raw_df__['isolated in'] = isolated_at.replace(to_replace_isolated_location,regex=True)
            self.__raw_df__['isolated district'] = isolated_at.replace(to_replace_isolated_district,regex=True)

    @Logger.log(name='Add Positive Result Date')
    def __add_positive_result_date__(self):
        self.__raw_df__['+ve result date'] = self.document.filename

    @Logger.log(name='Remove Redundant Columns')
    def __remove_redundant_cols__(self):
        cols = self.columns.get()
        self.__raw_df__.drop(columns=[cols['DPNO'],cols['SNO'],cols['ISOAT'],cols['SOURCE']],inplace=True,errors='ignore')
        self.__raw_df__.reset_index(drop=True,inplace=True)
    
    def __process__(self):
        self.__wrapper__(table.drop_unimportant_values,self.__raw_df__)
        self.columns.determine(self.__raw_df__)
        self.__wrapper__(table.clean_values,self.__raw_df__,[self.columns.get('ISOAT'),self.columns.get('SOURCE')])
        self.__wrapper__(rows.join,self.__raw_df__)
        self.__split_description__()
        self.__split_isolation_location__()
        self.__add_positive_result_date__()
        self.__remove_redundant_cols__()
        self.__format_sex__()
        self.__wrapper__(table.reset_columns,self.__raw_df__) 
    
    def __init__(self,doc):
        self.document = doc
        self.__raw_df__ = self.document.get_tables('NEW')

        self.columns = Columns(['PNO','SNO','DPNO','ISOAT','SOURCE','SEX','DISTRICT','AGE'])
        self.columns.set_frequencies(unit=['SNO','PNO','DPNO'],multiple=['SOURCE','ISOAT','AGE'])
        self.__process__()
    
    def get(self):
        return self.__raw_df__
    
def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

pdf_path = create_abs_path(f'/data/06-07/{file_name}.pdf')
doc = Document(pdf_path,file_name)
newcases = NewCases(doc)
df = newcases.get()
df