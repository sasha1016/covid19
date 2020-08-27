import pandas as pd
import numpy as np
import Logger
from math import floor

def row_mostly_null(row):
    values = row.values.tolist()
    size = row.size 
    size_wo_na = row.dropna().size 
    size_wo_na -= values.count('nan') + values.count(pd.NaT)
    row_mostly_null = ((size - size_wo_na) > floor(size / 2)) if size < 5 else ((size - size_wo_na) >= floor(size / 2))

    if size <= 2 and size_wo_na != 0:
        return False

    return row_mostly_null

    # if (most_frequent_value == 'nan' or pd.isna(most_frequent_value)) and counts.to_list()[0] != 1:
    #     if row_mostly_null:
    #         return True
        
    # else:
    #     return False

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
    disjoint_rows = list(reversed(disjoint_rows))
    for row in disjoint_rows:
        self.__raw_df__.loc[row] = self.__raw_df__.loc[row].replace({np.nan:'','nan':''})
        self.__raw_df__.loc[row-1] = self.__raw_df__.loc[row-1].replace({np.nan:'','nan':''})
        self.__raw_df__.loc[row-1] = self.__raw_df__.loc[row-1].apply(str) + ' ' + self.__raw_df__.loc[row].apply(str)
        if delimiters is not None:
            for column in delimiters:
                if self.__raw_df__.at[row,column] != '':
                    self.__raw_df__.at[row-1,column] = (self.__raw_df__.at[row-1,column] + delimiters[column] + self.__raw_df__.at[row,column])
        self.__raw_df__.loc[row] = np.nan
    self.__raw_df__.dropna(how='all',axis=0,inplace=True)
    self.__raw_df__ = self.__raw_df__.reset_index(drop=True)
    return disjoint_rows
        
def main():
    series= []
    series.append(pd.Series([np.nan,'h']))
    series.append(pd.Series([1,2]))
    series.append(pd.Series([np.nan,1,2]))
    series.append(pd.Series([np.nan,1,3,54,23]))
    series.append(pd.Series([np.nan,np.nan,np.nan,1,'23123',2]))
    series.append(pd.Series(['nan','nan',2]))
    series.append(pd.Series([np.nan,np.nan,np.nan,4,2,3]))
    series.append(pd.Series(['nan','nan','nan','nan','nan',5,5,4,2,3]))
    series.append(pd.Series([np.nan,np.nan,np.nan,np.nan,2,3]))

    i = 0 
    for s in series:
        print(f'series {i} {row_mostly_null(s)}')
        i += 1

if __name__ == '__main__':
    main()