import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd
import numpy as np
from Document import Document

pd.options.display.max_rows = 200

def row_mostly_null(row):
    if row.value_counts().index[0] == 'nan':
        return True
    else:
        return False


class NewCases(Document): 
    __raw_df__ = None 
    COLUMNS = {'DESCRIPTION':None}
    __reset_index_count__ = 0 
    __cols__ = {'SNO':None,'PNO':None,'DPNO':None,'ISOAT':None,'SOURCE':None,'SEX':None,'DISTRICT':None,'AGE':None}
    __DISTRICTS__ = {'dakshina kannada','belagavi','hassana','hassan','shivamogga','bagalakote', 'ballari','bellary','belagavi','gadag','vijayapura','chamarajanagara','kodagu','mysuru','mandya','chikkamagaluru','bidar','bengaluru urban','bengaluru rural','udupi','uttara kannada','dharwada','dharwad','yadgir','yadagiri','koppala','kalaburagi','raichuru','raichur','davanagere','bengaluru rural','chitradurga','ramanagara','ramnagar','chikkaballapura','kolara','kolar','tumkur','tumakuru'}
    __SEXES__ = {'male','female','f','m'}

    def __determine_columns__(self):

        FREQUENCY_ONE_REGEXES = [('SNO',re.compile(r'^\d{1,4}$',re.I)),('PNO',re.compile(r'^([Pp]\s*[-]?\s*)?(\d{4,10})$',re.I)),('DPNO',re.compile(r'^([\w]{2,4}\s*[-]?\s*)?(\d+)$',re.I))]
        FREQUENCY_MULTIPLE_REGEXES = [('SOURCE',re.compile(r'^(Contact|Inter|Returnee|ILI|SARI).*',re.I)),('ISOAT',re.compile(r'^((?:died|designated|private|brought).*(?=[-,]))(.*)',re.I)),('AGE',re.compile(r'^\d{1,2}$'))]

        for column in self.__raw_df__:
            value_counts = self.__raw_df__[column].value_counts().drop(labels=['nan'],errors='ignore')
            values, frequencies = value_counts.index.to_list(), value_counts.to_list() 
            slice_index = len(frequencies) if len(frequencies) < 25 else 25
            values,frequencies = values[0:slice_index], frequencies[0:slice_index]
            
            if(set(values).issubset(self.__SEXES__)): # Column has all only two values. can only be sex
                self.__cols__['SEX'] = column 
            else: 
                partof = lambda smaller_set: True if (len(smaller_set - self.__DISTRICTS__) < round(.2 * len(self.__DISTRICTS__))) else False
                if(partof(set(values))): #Should be a list of districts
                    self.__cols__['DISTRICT'] = column
                    continue
                i,j = 0, 0
                column_matched = False
                while not column_matched:
                    distinct_valued_column = frequencies.count(1) == len(frequencies) 
                    column_name,column_regex = FREQUENCY_MULTIPLE_REGEXES[j] if not distinct_valued_column else FREQUENCY_ONE_REGEXES[i]
                    matches = [True if column_regex.match(value) is not None else value for value in values]
                    if(matches.count(True) >= .75 * len(values)): # 75% of the column matches it 
                        self.__cols__[column_name] = column 
                        column_matched = True
                        break
                    else:
                        if j == len(FREQUENCY_MULTIPLE_REGEXES) - 1 or i == len(FREQUENCY_ONE_REGEXES):
                            break 
                        i += 1 if distinct_valued_column else 0
                        j += 1 if not distinct_valued_column else 0 
        
    def __drop_unimportant_values__(self):
        self.__raw_df__ =\
        self.__raw_df__\
        .replace('',np.nan)\
        .replace(regex = [r'\d\d\.\d\d\.\d\d\d\d',r'\bAnnexure\b',r'\bToday\b'],value = np.nan)\
        .dropna(how='all',axis=1).dropna(how='all').reset_index(drop=True)

    def __format_values__(self):
        self.__raw_df__.replace(regex=[r'\n',r'P-',r'&'],value=['','',','],inplace=True)
        self.__raw_df__= self.__raw_df__.apply(lambda x: x.astype(str).str.lower().str.strip())
    
    def __reset_columns__(self):
        columns = list(self.__raw_df__.iloc[0])
        columns = [re.sub(r'\s{1,}',' ',name) for name in columns]
        self.__raw_df__ = self.__raw_df__.iloc[1:]
        self.__raw_df__.columns = columns

    def __join_disjoint_rows__(self):
        disjoint_rows = [index for index,row in self.__raw_df__.iterrows() if row_mostly_null(row)]
        disjoint_rows_preceeding_rows = [index-1 for index in disjoint_rows]
        try:
            for row in disjoint_rows:
                self.__raw_df__.loc[row].replace('nan',' ',inplace=True)
                self.__raw_df__.loc[row-1] = self.__raw_df__.loc[row-1] + ' ' + (self.__raw_df__.loc[row])
                self.__raw_df__.loc[row] = np.nan
        except:
            print(row,row-1)
        self.disjoint_rows_preceeding_rows = disjoint_rows_preceeding_rows
        self.__raw_df__ = self.__raw_df__.dropna(how='all',axis=1).dropna(how='all')

    def __split_description__(self):
        source_col = self.__cols__['SOURCE']
        if source_col is not None:
            to_replace_source = {r'^contact.*':'contact', r'ili':'ili',r'sari':'sari',r'^(returnee|international).*':'returnee',r'^(inter|district|travel\s+history).*':'idt'}
            to_replace_meta = {r'^contact\s+(?:of|under)?\s*(?:[pP])?\s*[-–]?\s*(\w+\s*\w+|\w+|\d+)$':r'\1',r'^(returnee|international).* (from)? (.*)':r'\3',r'^(inter|district).*[-–,]\s*?(\w+|\w+-\w+|\w+\s+\w+)$':r'\2'}
            source = self.__raw_df__[source_col]
            self.__raw_df__['source'] = source.replace(to_replace_source,regex=True) 
            self.__raw_df__['meta'] = source.replace(to_replace_meta,regex=True).replace('tracing','tbd') 

    def __format_sex__(self):
        source_col = self.__cols__['SEX']
        print(source_col)
        if source_col is not None:
            sex = self.__raw_df__[source_col]
            sex.replace({'female':'f','male':'m'},inplace=True)

    def __split_isolation_location__(self):
        isoat_col = self.__cols__['ISOAT']
        if isoat_col is not None:
            to_replace_isolated_location = {r'^(?:brought|died)(?:.*)((?:\b\w+\b)\s*hospital|residence)(?:.*)':r'deceased - \1',r'^(\w+\s+\w+)[,-]?(.*)':r'\1'}
            to_replace_isolated_district = {r'^(?:brought|died).*(?:(?<=(?:her|his))|(?<=[,])|(?<=hospital))(.*)$':r'\1'}
            isolated_at = self.__raw_df__[isoat_col]
            self.__raw_df__['isolated in'] = isolated_at.replace(to_replace_isolated_location,regex=True)
            self.__raw_df__['isolated district'] = isolated_at.replace(to_replace_isolated_district,regex=True)

    def __add_positive_result_date__(self):
        self.__raw_df__['+ve result date'] = self.__date__ 

    def __remove_redundant_cols__(self):
        cols = self.__cols__
        self.__raw_df__.drop(columns=[cols['DPNO'],cols['SNO'],cols['ISOAT'],cols['SOURCE']],inplace=True,errors='ignore')
        self.__raw_df__.reset_index(drop=True,inplace=True)
    
    def __process__(self):
        self.__drop_unimportant_values__()
        self.__reset_columns__()
        self.__format_values__()
        self.__determine_columns__()
        self.__join_disjoint_rows__() 
        self.__split_description__()
        self.__split_isolation_location__()
        self.__add_positive_result_date__()
        self.__remove_redundant_cols__()
        self.__format_sex__()
    
    def __init__(self,path,filename):
        self.__date__ = filename 
        Document.__init__(self,path,filename)
        Document.get_tables(self,'NEW')
        self.__process__()
    
    def get(self):
        return self.__raw_df__
    
def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

file_name = '07-07-2020'
pdf_path = f'/data/06-07/{file_name}.pdf'
recovery = NewCases(create_abs_path(pdf_path),file_name)
df = recovery.get()
df
#df[df['source'] == 'tracing']
#df[df['district'] != df['isolated district']]