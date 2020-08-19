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
import constants as const
from Column import constants as col

pd.options.display.max_rows = 200

class NewCases(): 

    __raw_df__ = None
        
    @Logger.log(name='Split Source')
    def __split_description__(self):
        source_col = self.columns.get(col.SOURCE)
        if source_col is not None:
            to_replace_source = {r'^contact.*':'contact', r'ili':'ili',r'sari':'sari',r'^(returnee|international).*':'returnee',r'^(inter|district|travel\s+history).*':'idt'}
            to_replace_meta = {r'^contact\s+(?:of|under)?\s*(?:[pP])?\s*[-–]?\s*(\w+\s*\w+|\w+|\d+)$':r'\1',r'^(returnee|international).* (from)? (.*)':r'\3',r'^(inter|district).*[-–,]\s*?(\w+|\w+-\w+|\w+\s+\w+)$':r'\2'}
            source = self.__raw_df__[source_col]
            self.__raw_df__['source'] = source.replace(to_replace_source,regex=True) 
            self.__raw_df__['meta'] = source.replace(to_replace_meta,regex=True).replace('tracing','tbd') 

    @Logger.log(name='Format Sex')
    def __format_sex__(self):
        sex_col = self.columns.get(col.SEX)
        if sex_col is not None:
            self.__raw_df__[sex_col].replace({r'^[Ff].*':'f',r'^[Mm].*':'m'},regex=True,inplace=True)

    @Logger.log(name='Split Isolation Location')
    def __split_isolation_location__(self):
        isoat_col = self.columns.get(col.ISOAT)
        if isoat_col is not None:
            to_replace_isolated_location = {r'^(?:brought|died)(?:.*)((?:\b\w+\b)\s*hospital|residence)(?:.*)':r'deceased - \1',self.columns.COLUMNS[col.ISOAT]['REGEX']:r'\1'}
            to_replace_isolated_district = {r'^(?:brought|died|designated|private).*(?:(?<=(?:her|his))|(?<=[,])|(?<=hospital))(.*)$':r'\1'}
            isolated_at = self.__raw_df__[isoat_col]
            self.__raw_df__['isolated in'] = isolated_at.replace(to_replace_isolated_location,regex=True)
            self.__raw_df__['isolated district'] = isolated_at.replace(to_replace_isolated_district,regex=True)

    @Logger.log(name='Add Positive Result Date')
    def __add_positive_result_date__(self):
        self.__raw_df__['+ve result date'] = self.date

    @Logger.log(name='Remove Redundant Columns')
    def __remove_redundant_cols__(self):
        cols = self.columns.get([col.DPNO,col.SNO,col.ISOAT,col.SOURCE])
        self.__raw_df__.drop(columns=cols,inplace=True,errors='ignore')
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
    
    @table.initialize(table=const.NEWCASES)
    def __init__(self,doc):
        self.date = doc.filename

        self.columns = Columns([col.PNO,col.SNO,col.DPNO,col.ISOAT,col.SOURCE,col.SEX,col.DISTRICT,col.AGE])
        self.columns.set_frequencies(unit=[col.SNO,col.PNO,col.DPNO],multiple=[col.SOURCE,col.ISOAT,col.AGE])
    
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