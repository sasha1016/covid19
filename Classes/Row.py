import pandas as pd
import numpy as np
import Logger

def row_mostly_null(row):
    size = row.size 
    size_wo_na = row.dropna().size
    row_mostly_null = ((size - size_wo_na) / size) > .7

    counts = row.value_counts(dropna=False)
    most_frequent_value = counts.index[0]
    if ((most_frequent_value == 'nan' or pd.isna(most_frequent_value)) and counts.to_list()[0] != 1) and row_mostly_null:
        return True
    else:
        return False

# class Rows():

#     def join(self,self.__raw_df__,delimiters=None):
#         disjoint_rows = [index for index,row in self.__raw_df__.iterrows() if row_mostly_null(row)]
#         try:
#             for row in disjoint_rows:
#                 self.__raw_df__.loc[row] = self.__raw_df__.loc[row].replace({np.nan:'','nan':''})
#                 self.__raw_df__.loc[row-1] = self.__raw_df__.loc[row-1].apply(str) + ' ' + self.__raw_df__.loc[row].apply(str)
#                 if delimiters is not None:
#                     for column in delimiters:
#                         if self.__raw_df__.at[row,column] != '':
#                             self.__raw_df__.at[row-1,column] = (self.__raw_df__.at[row-1,column] + delimiters[column] + self.__raw_df__.at[row,column])
#                 self.__raw_df__.loc[row] = np.nan
#         except Exception as e:
#             print(e)
#         self.__raw_df__ = self.__raw_df__.dropna(how='all',axis=0)
#         return self.__raw_df__

#     def __init__(self):
#         print('asd')

@Logger.log(name='Join Rows',df=True)
def join(self,delimiters=None):
    disjoint_rows = [index for index,row in self.__raw_df__.iterrows() if row_mostly_null(row)]
    print(disjoint_rows)
    for row in disjoint_rows:
        self.__raw_df__.loc[row] = self.__raw_df__.loc[row].replace({np.nan:'','nan':''})
        self.__raw_df__.loc[row-1] = self.__raw_df__.loc[row-1].apply(str) + ' ' + self.__raw_df__.loc[row].apply(str)
        if delimiters is not None:
            for column in delimiters:
                if self.__raw_df__.at[row,column] != '':
                    self.__raw_df__.at[row-1,column] = (self.__raw_df__.at[row-1,column] + delimiters[column] + self.__raw_df__.at[row,column])
        self.__raw_df__.loc[row] = np.nan
    self.__raw_df__.dropna(how='all',axis=0,inplace=True)
    self.__raw_df__ = self.__raw_df__.reset_index(drop=True)
    return disjoint_rows
        