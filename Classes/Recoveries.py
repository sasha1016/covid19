import os 
import re
import pandas as pd
import numpy as np
from datetime import datetime
import time
import Logger

from Document import Document
import Row as rows
import Table as table
from Column import Columns 


pd.options.display.max_rows = 200

class Recoveries():

    __raw_df__ = None 

    @Logger.log(name='Fix Patient Numbers Above Table Head',df=True)
    def __fix_pnos_positional_errors__(self):

        thead_row, pnos_placement_errors = (None,None)

        def determine_thead(self):
            patient_numbers_coords = [(x, self.__raw_df__.columns[y])\
                for x, y in zip(*np.where(self.__raw_df__.values == 'patient number'))]
            #print(patient_numbers_coords)
            thead_row = patient_numbers_coords[0][0]
            return thead_row
        
        def shift(self):
            max_col = list(self.__raw_df__.columns)[-1]
            min_col = self.columns.get('PNOS')
            for col in range(max_col,min_col,-1):
                self.__raw_df__[col-1] = self.__raw_df__[col-1].replace(to_replace=['nan',np.nan], value=['','']).str.cat(self.__raw_df__[col].replace(to_replace=['nan',np.nan], value=['','']))
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

    def __checks__(self):

        def pnos_count(self):
            all_counts_match = True 
            for (index,check) in self.__raw_df__['check'].iteritems():
                if check == 0:
                    all_counts_match = False 
                    print(f'Patient Numbers Count failed for {index} row')
            if all_counts_match:
                print('All Patient Numbers Count match!')

        table.checks(self,pnos_count)

    @Logger.log(name='Clean Values and Strip strings')
    def __clean_values__(self,replace_pno=False):
        to_replace = {r'\n':'',r'&':',',r'[pP]\s*-\s*':''}
        self.__raw_df__.replace(to_replace,regex=True,inplace=True)        
        self.__raw_df__ = self.__raw_df__.apply(lambda x: x.astype(str).str.lower().str.strip())

    def __process__(self):
        table.drop_unimportant_values(self)
        self.__clean_values__()
        self.columns.determine(self.__raw_df__)
        self.__fix_pnos_positional_errors__()
        table.drop_unimportant_values(self)
        self.__drop_total_row__()
        rows.join(self)
        self.__split_snos_and_districts__()      
        self.__extract_total_from_district__() 
        self.__create_check_column__()
        table.reset_columns(self) 
        self.__checks__()


    def get(self):
        return self.__raw_df__ 

    def __init__(self,doc):
        table.IS = 'RECOVERIES'
        print('Recoveries parsing started')

        Logger.init(f'/data/logs/recoveries/{doc.filename}')

        if not doc.exists('RECOVERIES'):
            Logger.message(f'Recoveries table doesn\'t exist or is not readable')
            return None 

        start = time.perf_counter()
        self.__raw_df__ = doc.get_tables('RECOVERY',force=False)
        Logger.message(f'Took {time.perf_counter() - start}s to load Recoveries')

        self.columns = Columns(columns=['PNOS','SNO','DISTRICT','TOT'])
        self.columns.set_frequencies(unit=['SNO','PNOS','DISTRICT'] ,multiple=['TOT'])

        start = time.perf_counter()
        self.__process__() 
        Logger.message(f'Took {time.perf_counter() - start}s to process Recoveries')
        Logger.drop()
        print('Recoveries parsing completed')


def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def main():
    file_name = '08-07-2020'
    Logger.init(f'/data/logs/recoveries/{file_name}')
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




