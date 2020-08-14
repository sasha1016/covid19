import os 

from Document import Document 
from Recoveries import Recoveries
from NewCases import NewCases
from Deaths import Deaths

def create_abs_path(rel_path): 
    path_to_data = os.getcwd() + os.sep + os.pardir
    return (os.path.normpath(path_to_data + rel_path))

def csv_path(table,file_name):
    table = table.lower() 
    return create_abs_path(f'/data/csv/{table}/{file_name}.csv')

def main():
    file_name = '10-07-2020'
    pdf_path = f'/data/06-07/{file_name}.pdf'
    pdf_path = create_abs_path(pdf_path)

    doc = Document(pdf_path,file_name)
    recoveries = Recoveries(doc)
    newcases = NewCases(doc)
    deaths = Deaths(doc)

    recoveries.get().to_csv(csv_path('recoveries',file_name))
    newcases.get().to_csv(csv_path('newcases',file_name))
    deaths.get().to_csv(csv_path('deaths',file_name))

if __name__ == "__main__":
    main()