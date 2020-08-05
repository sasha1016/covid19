import os 
import camelot
import PyPDF2 as pypdf
import re
import pandas as pd 

def create_abs_path(rel_path): 
    return (os.path.normcase(os.getcwd() + rel_path))

file_name = '10-07-2020' 
text_files_dir = '/data/06-07/text'

#tables = camelot.read_pdf(create_abs_path(f'/data/06-07/{file_name}.pdf'),pages='4')
#tables = tables[0].df
#tables

pdf_path = create_abs_path(f'/data/06-07/{file_name}.pdf') 
def find_table_spans(pdf):
    spans = []
    found_first_table = False 
    for page_number in range(0,pdf.getNumPages()):
        annexure_reg_ex = re.compile(r'\bAnnexure',re.I)
        page = pdf.getPage(page_number).extractText()
        if re.search(annexure_reg_ex,page) is not None:
            spans.append(page_number + 1)
    return [(value,value) if len(spans)-1 == position else (value,spans[position+1] - 1) for position,value in enumerate(spans)]

def get_table(page_range):
    lower,upper = page_range 
    combined_df = None 
    for page_number in range(lower,upper + 1): 
        table = camelot.read_pdf(create_abs_path(f'/data/06-07/{file_name}.pdf'),pages=str(page_number))
        if page_number == lower: 
            combined_df = table[0].df
        else: 
            combined_df = pd.concat([combined_df,table[0].df])
            print(combined_df.shape)
    return combined_df
            
    return dfs
def main():
    pdf = pypdf.PdfFileReader(pdf_path)
    table_spans = [(4, 6), (7, 92), (93, 95), (96, 96)] # find_table_spans(pdf)
    tables = get_table(table_spans[0]) #[get_table(span) for span in table_spans]
    (tables)
    #print(table_spans)
main()
