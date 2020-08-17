import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
from regex import search

import Logger


def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

class Document: 
    __path__ = None
    __table_spans__ = []
    pdf = None
    TABLE_NAMES = ['RECOVERIES','NEWCASES','DEATHS','ICU']
    TABLES = {'RECOVERY':0,'NEW':1,'DEATHS':2,'ICU':3}
    EXISTS = {'RECOVERIES':False,'NEWCASES':False,'DEATHS':False,'ICU':False}
    UNIQUE_COLUMNS = {'RECOVERIES':None,'NEWCASES':None,'DEATHS':None,'ICU':None}
    UNIQUE_COLUMNS['RECOVERIES'] = ['District','Patient Number','Total']
    UNIQUE_COLUMNS['NEWCASES'] = ['Distict','State P Code','Isolated','District P Code','Source','Description']
    UNIQUE_COLUMNS['DEATHS'] = ['DOA','DOD','Co-Morbidities','Symptoms']
    UNIQUE_COLUMNS['ICU'] = ['District','Total','Patient Number']

    def __find_table_present__(self,text):
        whitespace_removed = re.sub(r'[\t\n]+','',text)

        results = {'SCORE':0,'MATCHED':0,'TABLE':None}
        for table in self.TABLE_NAMES:
            columns_matched = []
            for column in self.UNIQUE_COLUMNS[table]:
                permissible_error = '1' if len(column) <= 5 else '3'
                regex = f'{column}' + '{e<='+ permissible_error + '}'
                if search(regex,whitespace_removed,flags=re.I) is not None:
                    columns_matched.append(True)
                else:
                    columns_matched.append(False)

            if columns_matched.count(True) == results['MATCHED'] and results['TABLE'] is not None:
                if self.EXISTS[results['TABLE']]:
                    results['MATCHED'] = columns_matched.count(True)
                    results['TABLE'] = table
            elif columns_matched.count(True) > results['MATCHED']:
                results['MATCHED'] = columns_matched.count(True)
                results['TABLE'] = table
        
        if results['TABLE'] is not None:
            self.EXISTS[results['TABLE']] = True
        return results['TABLE']

    def exists(self,table):
        return self.EXISTS[table] 
    
    def __get_table_spans__(self):
        spans = []
        for page_number in range(0,self.pdf.getNumPages()):
            annexure_reg_ex = re.compile(r'\bAnnexure\b',re.I)
            page = self.pdf.getPage(page_number).extractText()
            matches = re.findall(annexure_reg_ex,page)
            if matches != []:
                spans.append(page_number + 1)
                multiple_tables = True if len(matches) > 1 else False
                print(matches,page_number)
                self.__find_table_present__(page)
        self.__table_spans__ = [(value,value) if len(spans) - 1 == position \
                                else (value,spans[position+1] - 1) \
                                for position,value in enumerate(spans)]
        print(self.EXISTS)
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

def main():
    file_name = '15-06-2020'
    pdf_path = f'/data/06-07/{file_name}.pdf'
    pdf_path = create_abs_path(pdf_path)

    doc = Document(pdf_path,file_name)

if __name__ == "__main__":
    main() 