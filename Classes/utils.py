import constants as const
import shutil 
import os
import logging,sys 

def print_success(msg):
    print(f'{const.SUCCESS}{msg}','\033[m')

def print_error(msg):
    print(f'{const.ERROR}{msg}','\033[m')

def print_br(char):
    string = f'{char * shutil.get_terminal_size()[0]}'
    print(string)

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def csv_path(table,file_name):
    table = table.lower() 
    return create_abs_path(f'/data/csv/{table}/{file_name}.csv')