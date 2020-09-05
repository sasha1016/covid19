import os 

from Document import Document 
from Recoveries import Recoveries
from NewCases import NewCases
from Deaths import Deaths

from utils import print_br,print_success,csv_path,print_error


def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def process_file(pdf_path,file_name):
    doc = Document(pdf_path,file_name)
    recoveries = Recoveries(doc)
    newcases = NewCases(doc)
    deaths = Deaths(doc)

    if recoveries.get() is not None:
        recoveries.get().to_csv(csv_path('recoveries',file_name))
    if newcases.get() is not None:
        newcases.get().to_csv(csv_path('newcases',file_name))
    if deaths.get() is not None:
        deaths.get().to_csv(csv_path('deaths',file_name))

    del doc, recoveries, newcases, deaths

def main():
    path_to_pdfs = f'/data/06-07'
    path_to_pdfs = create_abs_path(path_to_pdfs)
    files = os.listdir(path_to_pdfs)

    completed = []


    completed_file = open('completed.txt','r')
    with completed_file as f:
        for file_name in f:
            completed.append(file_name.rstrip('\n'))

    j = 0 

    for file_name in files:
        if j > 15:
            break 
        try:
            file_name = file_name.split('.')[0]
            if file_name in completed:
                print_br('/')
                print_success(f'\n{file_name}.pdf has already been processed\n')
            else:
                pdf_path = f'/data/06-07/{file_name}.pdf'
                process_file(create_abs_path(pdf_path),file_name)
                completed.append(file_name)
                j += 1 
        except:
            print_error(f'\nThere was an error while processing the file\n')
            continue


    completed_file = open('completed.txt','w+')
    with completed_file as f:
        for file_name in completed:
            f.write(f'{file_name}\n')

if __name__ == "__main__":
    main()