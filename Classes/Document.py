import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np

import Logger

# Make this a utlis function 

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

class Document: 
    __path__ = None
    __table_spans__ = []
    pdf = None
    TABLES = {'RECOVERY':0,'NEW':1,'DEATHS':2,'BED_STATUS':3}
    
    
    def __get_table_spans__(self):
        spans = []
        for page_number in range(0,self.pdf.getNumPages()):
            annexure_reg_ex = re.compile(r'\bAnnexure',re.I)
            page = self.pdf.getPage(page_number).extractText()
            if re.search(annexure_reg_ex,page) is not None:
                spans.append(page_number + 1)
        self.__table_spans__ = [(value,value) if len(spans) - 1 == position \
                                else (value,spans[position+1] - 1) \
                                for position,value in enumerate(spans)]
        return True
    
    def get_tables(self,table_name,force=False):
        combined_df = None
        try:
            if force:
                raise(KeyError)
            self.__raw_df__ = self.__get_df_from_hdf5__(table_name)
            combined_df = self.__raw_df__ 
        except (KeyError, FileNotFoundError):
            Logger.message(f'Got DF from PDF')
            lower,upper = self.__table_spans__[self.TABLES[table_name.upper()]]
            for page_number in range(lower,upper + 1): 
                table = camelot.read_pdf(self.__path__,pages=str(page_number))
                if page_number == lower: 
                    combined_df = table[0].df
                else: 
                    combined_df = pd.concat([combined_df,table[0].df],ignore_index=True)
            self.__raw_df__ = combined_df
            self.__save_df_to_hdf5__(table_name.upper())
        return combined_df

    def drop_unimportant_values(self):
        self.__raw_df__ =\
        self.__raw_df__\
        .replace('',np.nan)\
        .replace(regex = [r'\d\d\.\d\d\.\d\d\d\d',r'\bAnnexure\b',r'\bToday\b'],value = np.nan)\
        .dropna(how='all',axis=1).dropna(how='all').reset_index(drop=True)

    def clean_values(self):
        self.__raw_df__.replace(regex=[r'\n',r'P\s*-\s*',r'&'],value=['','',','],inplace=True)
        self.__raw_df__ = self.__raw_df__.apply(lambda x: x.astype(str).str.lower().str.strip())

    def reset_columns(self):
        actual_columns = self.__raw_df__.columns 
        columns_to_be = list(self.__raw_df__.iloc[0])
        columns = list(zip(actual_columns,columns_to_be))
        columns = [x if isinstance(x,str) else y for x,y in columns]
        columns = [re.sub(r'\s{1,}',' ',name) for name in columns]
        self.__raw_df__ = self.__raw_df__.iloc[1:]
        self.__raw_df__.columns = columns

    def row_mostly_null(self,row):
        if row.value_counts().index[0] == 'nan':
            return True
        else:
            return False

    def __save_df_to_hdf5__(self,table):
        Logger.message(f'Saved DF to hdf5')
        self.__raw_df__.to_hdf(self.__hdf5_path__,key = table)

    def __get_df_from_hdf5__(self,table):
        Logger.message(f'Got DF from hdf5')
        return pd.read_hdf(self.__hdf5_path__,key=table) 

    def __mk_hdf5_path__(self,filename):
        self.__hdf5_path__ = create_abs_path(f'/data/h5/{filename}.h5')

    def __init__(self,path,filename):
        self.filename = filename
        self.__path__ = path
        self.__mk_hdf5_path__(filename)
        self.pdf = pypdf.PdfFileReader(path)
        self.__get_table_spans__()
