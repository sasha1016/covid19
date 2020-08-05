import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
from datetime import datetime

from Document import Document
from Row import Rows
from Column import Columns 
import Logger

pd.options.display.max_rows = 200
file_name = '11-07-2020'

class Recoveries():

    __raw_df__ = None 

    def __fix_pnos_positional_errors__(self):

        thead_row, pnos_placement_errors = (None,None)

        def determine_thead(self):
            patient_numbers_coords = [(x, self.__raw_df__.columns[y])\
                for x, y in zip(*np.where(self.__raw_df__.values == 'Patient Number'))]
            #print(patient_numbers_coords)
            thead_row = patient_numbers_coords[0][0]
            return thead_row
        
        def shift(self):
            max_col = list(self.__raw_df__.columns)[-1]
            min_col = self.columns.get('PNOS')
            for col in range(max_col,min_col,-1):
                print(col)
                self.__raw_df__[col-1] = self.__raw_df__[col-1].str.cat(self.__raw_df__[col].replace(np.nan,''), '')
                self.__raw_df__[col] = np.nan

        def determine_errors(self,thead_row):
            pnos_col = self.columns.get('PNOS')
            pnos_regex = re.compile(self.columns.COLUMNS['PNOS']['REGEX'],re.I)
            placement_errors = [x for x,value in self.__raw_df__[pnos_col].iteritems()\
                if x < thead_row and isinstance(value,str) and pnos_regex.match(value) is not None]
            return placement_errors
        
        def fix(self,pnos_placement_errors,thead_row):
            index_district_pnos_row, pnos_col = (thead_row + 1 , self.columns.get('PNOS'))
            for row in pnos_placement_errors:
                new_value = self.__raw_df__.at[row,pnos_col] + self.__raw_df__.at[index_district_pnos_row,pnos_col]
                self.__raw_df__.at[index_district_pnos_row,pnos_col] = new_value
                self.__raw_df__.at[row,pnos_col] = np.nan 
        
        thead_row = determine_thead(self) 
        shift(self)
        pnos_placement_errors = determine_errors(self,thead_row) 
        fix(self,pnos_placement_errors,thead_row)

    def __drop_total_row__(self):
        self.__raw_df__ = self.__raw_df__.iloc[0:-1]


    def __extract_total_from_district__(self):
        district = self.columns.get()

            
    def __split_snos_and_districts__(self):
        snos,district = self.columns.get('SNO'), self.columns.get('DISTRICT')
        modded_snos = self.__raw_df__[snos].str.strip().str.extract(self.columns.COLUMNS['SNO']['REGEX']).replace(['nan',np.nan],'')
        modded_district = self.__raw_df__[district].str.strip().str.extract(self.columns.COLUMNS['SNO']['REGEX']).replace(['nan',np.nan],'')
        new_df = modded_snos.add(modded_district,fill_value = '')
        #print(new_df,modded_district,modded_snos)
        self.__raw_df__[snos].iloc[1:] = (new_df[0]) 
        self.__raw_df__[district].iloc[1:] = (new_df[1])

    def __process__(self):
        Document.drop_unimportant_values(self)
        #Document.clean_values(self)
        self.columns.determine(self.__raw_df__)
        self.__fix_pnos_positional_errors__()
        Document.drop_unimportant_values(self)
        Document.clean_values(self)
        self.__drop_total_row__()
        self.__raw_df__ = self.rows.join(self.__raw_df__)
        #print(self.columns.get())
        #Document.reset_columns(self)  
        self.__split_snos_and_districts__()      

    def get(self):
        return self.__raw_df__ 

    def __init__(self,document):
        self.document = document 
        self.__raw_df__ = self.document.get_tables('RECOVERY')

        self.columns = Columns(columns=['PNOS','SNO','DISTRICT','TOT'])
        self.columns.set_frequencies(unit=['SNO','PNOS'] ,multiple=['TOT'])
        self.rows = Rows() 

        self.__process__() 

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

pdf_path = f'/data/06-07/{file_name}.pdf'
pdf_path = create_abs_path(pdf_path)
doc = Document(pdf_path,file_name)
rec = Recoveries(doc)
df = rec.get()
df




