import os 
import re
import pandas as pd
import numpy as np
from datetime import datetime
from timeit import default_timer as timer 
import Logger

from Document import Document
import Row as rows
import Table as table
from Column import Columns 


pd.options.display.max_rows = 200

class Recoveries():

    __raw_df__ = None 

    @Logger.log(name='WRAPPER',wrapper=True,df=True)
    def __wrapper__(self,func,*args,**kwargs):
        logger_message = func(*args,**kwargs)
        return logger_message

    @Logger.log(name='Fix Patient Numbers Above Table Head',df=True)
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

    @Logger.log(name='Drop Total Row')
    def __drop_total_row__(self):
        self.__raw_df__ = self.__raw_df__.iloc[0:-1]

    @Logger.log(name='Extract Total From District',df=True)
    def __extract_total_from_district__(self):
        district,total = self.columns.get('DISTRICT'), self.columns.get('TOT')

        if district is not None and total is None: 
            regex = self.columns.COLUMNS['DISTRICT']['REGEX']
            df = self.__raw_df__[district].str.strip().str.extract(regex)
            districts, totals = df[0], df[1]
            self.__raw_df__[district] = districts 
            self.__raw_df__['total'] = totals 

    @Logger.log(name='Splitting Serial Numbers and Districts',df=True)        
    def __split_snos_and_districts__(self):
        snos,district = self.columns.get('SNO'), self.columns.get('DISTRICT')
        modded_snos = self.__raw_df__[snos].str.strip().str.extract(self.columns.COLUMNS['SNO']['REGEX']).replace(['nan',np.nan],'')
        modded_district = self.__raw_df__[district].str.strip().str.extract(self.columns.COLUMNS['SNO']['REGEX']).replace(['nan',np.nan],'')
        new_df = modded_snos.add(modded_district,fill_value = '')
        modded_districts_is_empty = modded_district.iloc[1:].replace('',np.nan).dropna(how='all').empty
        if not modded_districts_is_empty: 
            self.__raw_df__[snos].iloc[1:] = (new_df[0])
            self.__raw_df__[district].iloc[1:] = (new_df[1])
        else:
            self.__raw_df__[snos].iloc[1:] = (new_df[0])

    @Logger.log(name='Create column to check if Patient Numbers count and total count match')
    def __create_check_column__(self):
        pnos,total = self.columns.get('PNOS'), self.columns.get('TOT')
        total = total if total is not None else 'total'
        patientnos = self.__raw_df__[pnos].replace(regex=True,to_replace=[r'[.&]', r',\s*',r'\s+'],value=['',' ',' ']).str.rstrip()
        #print(patientnos[3],total,'\n\n',self.__raw_df__)
        self.__raw_df__['total pnos'] = patientnos.astype(str).str.split(' ').agg([len]).astype(float)
        totals = pd.to_numeric(self.__raw_df__[total],errors='coerce')
        self.__raw_df__['check'] = np.where(self.__raw_df__['total pnos'] == totals,1,0)

    @Logger.log(name='Clean Values and Strip strings')
    def __clean_values__(self,replace_pno=False):
        to_replace = {r'\n':'',r'&':',',r'[pP]\s*-\s*':''}
        self.__raw_df__.replace(to_replace,regex=True,inplace=True)        
        self.__raw_df__ = self.__raw_df__.apply(lambda x: x.astype(str).str.lower().str.strip())

    def __process__(self):
        start = timer() 
        self.__wrapper__(table.drop_unimportant_values,self.__raw_df__)
        self.__clean_values__()
        self.columns.determine(self.__raw_df__)
        print(self.columns.get())
        self.__fix_pnos_positional_errors__()
        self.__wrapper__(table.drop_unimportant_values,self.__raw_df__)
        self.__drop_total_row__()
        self.__wrapper__(rows.join,self.__raw_df__)
        self.__split_snos_and_districts__()      
        self.__extract_total_from_district__() 
        self.__create_check_column__()
        self.__wrapper__(table.reset_columns,self.__raw_df__) 
        print(f'\nTook {start - timer()}\'s to parse Recoveries')

    def get(self):
        return self.__raw_df__ 

    def __init__(self,doc):
        start = timer() 
        self.__raw_df__ = doc.get_tables('RECOVERY')
        print(f'\nTook {start - timer()}\'s to load recoveries')

        self.columns = Columns(columns=['PNOS','SNO','DISTRICT','TOT'])
        self.columns.set_frequencies(unit=['SNO','PNOS','DISTRICT'] ,multiple=['TOT'])

        Logger.init(f'/data/logs/recoveries/{doc.filename}')

        print('Started Parsing Recoveries')

        self.__process__() 

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def main():
    file_name = '08-07-2020'
    Logger.init(f'/data/logs/{file_name}')
    pdf_path = f'/data/06-07/{file_name}.pdf'
    pdf_path = create_abs_path(pdf_path)
    doc = Document(pdf_path,file_name)
    rec = Recoveries(doc)
    df = rec.get()
    df

if __name__ == "__main__":
    main()
else:
    file_name = '08-07-2020'
    Logger.init(f'/data/logs/recoveries/{file_name}')




