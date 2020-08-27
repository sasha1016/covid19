import os 
import camelot
import PyPDF2 as pypdf
import re
import regex
import pandas as pd
import numpy as np
from regex import search
import constants as const
from utils import print_br,print_success,print_error

import Logger

TABLES = [const.RECOVERIES,const.NEWCASES,const.DEATHS,const.ICU]

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

class Document: 

    def __find_table_present__(self,text):
        whitespace_removed = re.sub(r'[\t\n]+','',text)

        results = {'MATCHED':0,'TABLE':None}
        for table in TABLES:

            columns_matched = []
            for column in self.UNIQUE_COLUMNS[table]:
                #permissible_error = '1' if len(column) <= 5 else '3'
                regex = f'{column}'
                if search(regex,whitespace_removed,flags=re.I) is not None:
                    columns_matched.append(True)
                else:
                    columns_matched.append(False)

            if columns_matched.count(True) == results['MATCHED'] and results['TABLE'] is not None:
                if columns_matched[-1] == True:
                    results['MATCHED'] = columns_matched.count(True)
                    results['TABLE'] = table
            elif columns_matched.count(True) > results['MATCHED'] and (columns_matched.count(True) / len(columns_matched) >= 0.5):
                if table == const.DEATHS and columns_matched[-1] != True:
                    continue
                if self.EXISTS[table]:
                    continue
                results['MATCHED'] = columns_matched.count(True)
                results['TABLE'] = table

        if results['TABLE'] is not None:
            self.EXISTS[results['TABLE']] = True
        return results['TABLE']

    def exists(self,table):
        return self.EXISTS[table] 

    @Logger.log(name='Get Table Spans')
    def __get_table_spans__(self):
        self.__table_spans__ = {const.RECOVERIES:(0,),const.NEWCASES:(0,),const.DEATHS:(0,),const.ICU:(0,)}
        spans = {}
        for page_number in range(0,self.pdf.getNumPages()):
            page = self.pdf.getPage(page_number).extractText()
            matches = regex.findall(r'Annexure{e<=1}',re.sub(r'[\t\n]+','',page),re.I)
            if matches != []:
                table = self.__find_table_present__(page)
                if table is not None:
                    spans[page_number + 1] = table

        spans = list(spans.items())

        for index,values in enumerate(spans):
            page_number,table = values
            if len(spans) - 1 == index:
                self.__table_spans__[table] = (page_number,self.pdf.getNumPages())
            else:
                self.__table_spans__[table] = (page_number,(spans[index+1])[0]-1)

        for table,exists in list(self.EXISTS.items()):
            if exists:
                print_success(f'{const.DISPLAY_NAMES[table]} was found from pages {self.__table_spans__[table][0]} to {self.__table_spans__[table][1]}')
            else:
                print_error(f'{const.DISPLAY_NAMES[table]} was not found.')
    
        Logger.message(f'Table spans are {self.__table_spans__}')
    
    def get_tables(self,table_name,force=False):
        combined_df = None
        try:
            if force:
                raise(KeyError)
            self.__raw_df__ = self.__get_df_from_hdf5__(table_name)
            combined_df = self.__raw_df__ 
        except (KeyError, FileNotFoundError):
            Logger.message(f'Got DF from PDF')
            lower,upper = self.__table_spans__[table_name.upper()]
            columns_in_page = None
            for page_number in range(lower,upper + 1): 
                table = camelot.read_pdf(self.__path__,pages=str(page_number))
                df = table[0].df 
                if columns_in_page is None:
                    columns_in_page = df.shape[1] 
                else:
                    if columns_in_page != df.shape[1]:
                        #print(f'inserted a filler col {columns_in_page} already set and found {df.shape[1]} columns')
                        Logger.debug(f'\nInserted a filler col {columns_in_page} already set and found {df.shape[1]} columns\n{df.to_string()}')
                        columns = list(df.columns) 
                        columns = [c + 1 for c in columns]
                        df.columns = columns 
                        df.insert(loc=0,column=0,value='')
                Logger.debug(f'Table retrieved for page {page_number} is \n{df.to_string()}')
                if page_number == lower: 
                    combined_df = df
                else: 
                    combined_df = pd.concat([combined_df,df],ignore_index=True)
            self.__raw_df__ = combined_df
            self.__save_df_to_hdf5__(table_name.upper())
        return combined_df

    def __save_df_to_hdf5__(self,table):
        Logger.message(f'Saved DF to hdf5')
        self.__raw_df__.to_hdf(self.__hdf5_path__,key = table)

    def __get_df_from_hdf5__(self,table):
        Logger.message(f'Got DF from hdf5')
        return pd.read_hdf(self.__hdf5_path__,key=table) 

    def __mk_hdf5_path__(self,filename):
        self.__hdf5_path__ = create_abs_path(f'/data/h5/{filename}.h5')

    def __init__(self,path,filename):
        print_br('/')
        print(f'\nReading {filename}...\n')


        Logger.init(f'/data/logs/documents/{filename}')

        self.EXISTS = {const.RECOVERIES:False,const.NEWCASES:False,const.DEATHS:False,const.ICU:False}
        self.UNIQUE_COLUMNS = {const.RECOVERIES:['District','(Patient Number){e<=2}','(Total){e<=2}'],\
            const.NEWCASES:['District{e<=2}','(State\s*(?:P|Patient)\s*(?:Code|Number|No.)){e<=2}','(Isolated at){e<=2}','(District\s*(?:P|Patient)\s*(?:Code|Number|No.){e<=2})','(Source|History|Description){e<=1}'],\
                const.DEATHS:['DOA','DOD','Symptoms','Co-Morbidities{e<=1}'],\
                    const.ICU:['District','Total','Patient Number','ICU']}

        self.filename = filename
        self.__path__ = path
        self.__mk_hdf5_path__(filename)
        self.pdf = pypdf.PdfFileReader(path)
        self.__get_table_spans__()


        Logger.drop()
        print('')

def main():
    file_name = '25-06-2020'
    pdf_path = f'/data/06-07/{file_name}.pdf'
    pdf_path = create_abs_path(pdf_path)

    doc = Document(pdf_path,file_name)

if __name__ == "__main__":
    main() 