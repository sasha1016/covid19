import os 
import re
import regex
import pandas as pd
import numpy as np
from datetime import datetime
import time
import Logger

from Document import Document
import Row as rows
import Table as table
from Column import Columns 
from Column import constants as col
import constants as const
from utils import print_success,print_error,csv_path

pd.options.display.max_rows = 200

class Recoveries():

    __raw_df__ = None 

    @Logger.log(name='Fix Patient Numbers Above Table Head',df=True)
    def __fix_pnos_positional_errors__(self):

        thead_row, pnos_placement_errors = (None,None)

        def determine_thead(self):
            thead_row = None
            rows_to_search_in = self.__raw_df__.head(5)
            found = False
            for index,row in rows_to_search_in.iterrows():
                for string in row.values:
                    if isinstance(string,str):
                        if regex.search(r'(patient\s*number){e<=3}',string,flags=re.I) is not None:
                            thead_row = index
                            found = True 
                            break
                        else:
                            pass
                if found == True:
                    break
            return thead_row
        
        def shift(self):
            global col
            max_col = list(self.__raw_df__.columns)[-1]
            min_col = self.columns.get(col.PNOS)
            for column in range(max_col,min_col,-1):
                self.__raw_df__[column-1] = self.__raw_df__[column-1].replace(to_replace=['nan',np.nan], value=['','']).str.cat(self.__raw_df__[column].replace(to_replace=['nan',np.nan], value=['','']))
                self.__raw_df__[column] = np.nan

        def determine_errors(self,thead_row):
            pnos_col = self.columns.get(col.PNOS)
            pnos_regex = re.compile(self.columns.COLUMNS[col.PNOS]['REGEX'],re.I)
            placement_errors = [x for x,value in self.__raw_df__[pnos_col].iteritems()\
                if x < thead_row and isinstance(value,str) and pnos_regex.match(value) is not None]
            return placement_errors
        
        def fix(self,pnos_placement_errors,thead_row):
            index_district_pnos_row, pnos_col = (thead_row + 1 , self.columns.get(col.PNOS))
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
        district,total = self.columns.get([col.DISTRICT,col.TOT])

        if district is not None and total is None: 
            regex = self.columns.COLUMNS[col.DISTRICT]['REGEX']
            df = self.__raw_df__[district].str.strip().str.extract(regex)
            districts, totals = df[0], df[1]
            self.__raw_df__[district] = districts 
            self.__raw_df__['total'] = totals 

    @Logger.log(name='Splitting Serial Numbers and Districts',df=True)        
    def __split_snos_and_districts__(self):
        snos,district = self.columns.get([col.SNO,col.DISTRICT])
        modded_snos = self.__raw_df__[snos].str.strip().str.extract(self.columns.COLUMNS[col.SNO]['REGEX']).replace(['nan',np.nan],'')
        modded_district = self.__raw_df__[district].str.strip().str.extract(self.columns.COLUMNS[col.SNO]['REGEX']).replace(['nan',np.nan],'')
        new_df = modded_snos.add(modded_district,fill_value = '')
        modded_districts_is_empty = modded_district.iloc[1:].replace('',np.nan).dropna(how='all').empty
        if not modded_districts_is_empty: 
            self.__raw_df__[snos].iloc[1:] = (new_df[0])
            self.__raw_df__[district].iloc[1:] = (new_df[1])
        else:
            self.__raw_df__[snos].iloc[1:] = (new_df[0])

    @Logger.log(name='Create column to check if Patient Numbers count and total count match')
    def __create_check_column__(self):
        pnos,total = self.columns.get([col.PNOS,col.TOT])
        total = total if total is not None else 'total'
        patientnos = self.__raw_df__[pnos].replace(regex=True,to_replace=[r'[.&]', r',\s*',r'\s+'],value=['',' ',' ']).str.rstrip()
        self.__raw_df__['total pnos'] = patientnos.astype(str).str.split(' ').agg([len]).astype(float)
        totals = pd.to_numeric(self.__raw_df__[total],errors='coerce')
        self.__raw_df__['check'] = np.where(self.__raw_df__['total pnos'] == totals,1,0)

    def __checks__(self):

        def pnos_count(self):
            all_counts_match = True 
            for (index,check) in self.__raw_df__['check'].iteritems():
                if check == 0:
                    all_counts_match = False 
                    print_error(f'Patient Numbers Count failed for {index} row')
            if all_counts_match:
                print_success('All Patient Numbers Count match!')

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

    @table.initialize(table=const.RECOVERIES)
    def __init__(self,doc):

        self.columns = Columns(columns=[col.PNOS,col.SNO,col.DISTRICT,col.TOT])
        self.columns.set_frequencies(unit=[col.SNO,col.PNOS,col.DISTRICT] ,multiple=[col.TOT])


def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def main():
    file_name = '06-06-2020'
    Logger.init(f'/data/logs/recoveries/{file_name}')
    pdf_path = f'/data/06-07/{file_name}.pdf'
    pdf_path = create_abs_path(pdf_path)
    doc = Document(pdf_path,file_name)
    rec = Recoveries(doc)
    rec.get().to_csv(csv_path('recoveries',file_name))

if __name__ == "__main__":
    main()




