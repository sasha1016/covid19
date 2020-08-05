import re
import numpy as np

DISTRICTS = {\
    'dakshina kannada','belagavi','hassana','hassan','shivamogga','bagalakote',\
        'ballari','bellary','belagavi','gadag','vijayapura','chamarajanagara','kodagu',\
            'mysuru','mandya','chikkamagaluru','bidar','bengaluru urban','bengaluru rural','udupi',\
                'uttara kannada','dharwada','dharwad','yadgir','yadagiri','koppala','kalaburagi',\
                    'raichuru','raichur','davanagere','bengaluru rural','chitradurga','ramanagara',\
                        'ramnagar','chikkaballapura','kolara','kolar','tumkur','tumakuru'\
                            }
DISTRICTS_TOLERANCE_LIMIT = 7 
SEXES = {'male','female','f','m'}

COL_SEX = {'REGEX':r'^(Male|Female|f|m)$','VALUES':SEXES}
COL_SNO = {'REGEX':r'^(\d{1,5})?\s*(.*)?$','values':None}
COL_TOT = {'REGEX':r'^\d{1,5}$','values':None}
COL_PNO = {'REGEX':r'^([Pp]\s*[-]?\s*)?(\d{4,10})$','values':None}
COL_AGE = {'REGEX':r'^\d{1,2}$','values':None}
COL_DOA = {'REGEX':r'(?:brought|died\s*\w+)?((?:\d{1,2})\s*[-/.]\s*(?:\w{2,3}|\d{1,2})\s*[-/.]\s*(?:\d{2,4}))','values':None}
COL_DOD = {'REGEX':r'(?:brought|died\s*\w+)?((?:\d{1,2})\s*[-/.]\s*(?:\w{2,3}|\d{1,2})\s*[-/.]\s*(?:\d{2,4}))','values':None}
COL_DPNO = {'REGEX':r'^([\w]{2,4}\s*[-]?\s*)?(\d+)$','values':None}
COL_PNOS = {'REGEX':r'(?:(?:[Pp]\s*[-]?\s*)?(\d{1,10}),?)','values':None}
COL_ISOAT = {'REGEX':r'^((?:died|designated|private|brought).*(?=[-,]))(.*)','values':None}
COL_SOURCE = {'REGEX':r'(?:Contact|Inter|Returnee|ili|sari|Under|Asymptomatic).*','values':None}
COL_CMRBDTS = {'REGEX':r'(?:dm|htn|-|ihd|ckd)','values':None}
COL_SYMPTMS = {'REGEX':r'(breathlessness|fever|cough|cold)','values':None}
COL_DISTRICT = {'REGEX':r'^(?:.*)\s*?\((\d*)\)$','VALUES':DISTRICTS}

class Columns():

    COLUMNS = {\
        'SEX':COL_SEX,'SNO':COL_SNO,'PNO':COL_PNO,'AGE':COL_AGE,'DPNO':COL_DPNO,'TOT':COL_TOT,\
            'ISOAT':COL_ISOAT,'SOURCE':COL_SOURCE,'DISTRICT':COL_DISTRICT,'PNOS':COL_PNOS,\
                'DOA':COL_DOA,'DOD':COL_DOD,'CMRBDTS':COL_CMRBDTS,'SYMPTMS':COL_SYMPTMS,\
                    }

    __UNIT_FREQUENCY__ = None
    __MULTIPLE_FREQUENCY__ = None

    columns = {}

    def set_frequencies(self,unit=[],multiple=[]):
        self.__UNIT_FREQUENCY__ = [(column,re.compile(self.COLUMNS[column]['REGEX'],re.I)) for column in unit]
        self.__MULTIPLE_FREQUENCY__ = [(column,re.compile(self.COLUMNS[column]['REGEX'],re.I)) for column in multiple]


    def __column_is_district__(self,values):

        values = set([str(value).lower() for value in values])
        column_almost_subset = len(values - self.COLUMNS['DISTRICT']['VALUES']) < DISTRICTS_TOLERANCE_LIMIT
        has_common_values_with_districts = values.intersection(DISTRICTS) != set()

        return True if column_almost_subset and has_common_values_with_districts else False 

    def determine(self,df):
        if self.__UNIT_FREQUENCY__ is None:
            raise('Columns cannot be determined without setting single and multiple frequency columns')
        else:

            for column in df:
                value_counts = df[column].value_counts().drop(labels=['nan',np.nan,''],errors='ignore')
                values, frequencies = value_counts.index.to_list(), value_counts.to_list() 
                slice_index = len(frequencies) if len(frequencies) < 25 else 25
                values,frequencies = values[0:slice_index], frequencies[0:slice_index]

                values_set = set(values) 

                if(values_set.issubset(self.COLUMNS['SEX']['VALUES'])): 
                    # check if sex exists in the columns of the table 
                    if 'SEX' in self.columns:
                        self.columns['SEX'] = column 
                else: 
                
                    if(self.__column_is_district__(values_set)): #Should be a list of districts
                        self.columns['DISTRICT'] = column
                        continue

                    i,j = 0, 0
                    distinct_valued_column = frequencies.count(1) == len(frequencies) 
                    column_matched = False
                    while not column_matched:
                        column_name,column_regex = self.__MULTIPLE_FREQUENCY__[j] if not distinct_valued_column else self.__UNIT_FREQUENCY__[i]
                        matches = [True if column_regex.match(value) is not None else False for value in values]
                        #print(f'pitting {column} against {column_name} and column is distinct {distinct_valued_column}',matches)
                        if(matches.count(True) >= .75 * len(values) and not (column == 'dod' and column_name == 'DOA')): # 75% of the column matches it 
                            #print(f'Matched {column} with {column_name}')
                            if column_name == 'PNOS' and len(values) < 5:
                                break
                            self.columns[column_name] = column 
                            column_matched = True
                            break
                        else:
                            multiple_frequencies_traversed = (j == len(self.__MULTIPLE_FREQUENCY__) - 1)
                            unit_frequencies_traversed = (i == len(self.__UNIT_FREQUENCY__) - 1)
                            if multiple_frequencies_traversed and unit_frequencies_traversed:
                                break 
                            #print(multiple_frequencies_traversed,unit_frequencies_traversed,i,j,distinct_valued_column)
                            if distinct_valued_column:
                                if unit_frequencies_traversed:
                                    break
                                i += 1 

                            if not distinct_valued_column:
                                if multiple_frequencies_traversed:
                                    break
                                j += 1 
                            #j += 1 if not distinct_valued_column else 0

    
    def format_values(self,column,df,custom_regex = False,regex=r'a^'):
        regex = regex if custom_regex is True else self.COLUMNS[column]['REGEX']
        if column == 'DOA':
            df_column = self.columns[column]
            for (index,doa) in df[df_column].items():
                matches = re.findall(regex,doa) 
                if len(matches) == 2:
                    df.at[index,df_column] = matches[0]
                    df.at[index,self.columns['DOD']] = matches[1]

    def get(self,column = None):
        return self.columns[column] if column is not None else self.columns


    def __init__(self,columns):
        for column in columns:
            self.columns[column] = None
            