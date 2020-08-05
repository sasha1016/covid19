import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
from dateutil import parser
from datetime import datetime

from Document import Document
from Row import Rows
from Column import Columns 
import Logger

pd.options.display.max_rows = 200
file_name = '10-07-2020'
Logger.init(f'/data/logs/{file_name}')

class Deaths():

    __raw_df__ = None 

    @Logger.log(name='Replace Hyphen',message='Relaced all hyphens',df=True)
    def __replace_hyphens__(self):
        self.__raw_df__.replace('-','',inplace=True)

    @Logger.log(name='Add Dead on Arrival',message="Dead on Arrival column added for all rows",df=True)
    def __add_dead_on_arrival_column__(self):
        regex = re.compile(r'^brought|died.*',re.I)
        column_name = self.columns.get('DOA')
        doa = self.__raw_df__[column_name]
        self.__raw_df__['dead on arrival'] = np.where(doa.str.contains(regex), 1 , 0)

    @Logger.log(name="Format DOA",message="Converted all Date of Admissions")
    def __format_doa__(self):
        regex = self.columns.COLUMNS['DOA']['REGEX']
        doa = self.columns.get('DOA')
        self.__raw_df__[doa] = self.__raw_df__[doa].str.extract(regex)
        date_components_regex = re.compile(r'(?:brought|died\s*\w+)?(?:(\d{1,2})\s*[-/.]\s*(\w{2,3}|\d{1,2})\s*[-/.]\s*(\d{2,4}))',re.I)
        self.__raw_df__[doa].replace(to_replace=date_components_regex,value=r'\1-\2-\3', inplace=True)
    
    @Logger.log(name="Join",message="Disjoint rows joined",df=True)
    def __join__(self):
        symptoms,comobs = self.columns.get('SYMPTMS'), self.columns.get('CMRBDTS')
        delimiters = {} 
        delimiters[symptoms], delimiters[comobs] = ',', ',' 
        self.rows.join(self.__raw_df__,delimiters) 
        self.__raw_df__.dropna(axis=0,how='all',inplace=True)
    
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

    def __process__(self):
        Document.drop_unimportant_values(self)
        Document.clean_values(self) 
        Document.reset_columns(self)
        self.columns.determine(self.__raw_df__)
        self.columns.format_values(column='DOA',df = self.__raw_df__)
        self.__replace_hyphens__() 
        self.__add_dead_on_arrival_column__()
        self.__join__()
        self.__raw_df__ = self.__raw_df__.reset_index(drop=True)
        self.__format_doa__()
        self.__convert_to_datetime__('DOA')
        self.__convert_to_datetime__('DOD')
        self.__add_duration__()
        #self.__explode__('SYMPTMS')

    def __init__(self,path,filename):
        self.__date__ = filename 
        Document.__init__(self,path,filename)
        Document.get_tables(self,'DEATHS')

        self.columns = Columns(['SNO','PNO','DISTRICT','AGE','SEX','SOURCE','SYMPTMS','CMRBDTS','DOA','DOD'])
        self.columns.set_frequencies(unit=['SNO','PNO'],multiple=['AGE','SOURCE','CMRBDTS','DOA','DOD','SYMPTMS'])

        self.rows = Rows()

        self.__process__()
    
    def get(self):
        return self.__raw_df__
    
def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))


pdf_path = f'/data/06-07/{file_name}.pdf'
deaths = Deaths(create_abs_path(pdf_path),file_name)
df = deaths.get()
df