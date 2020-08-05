import datetime as dt
import os 
import requests 
from multiprocessing.pool import ThreadPool

start = dt.datetime.strptime('06-06-2020','%d-%m-%Y') 
end = dt.datetime.strptime('12-07-2020','%d-%m-%Y')

dates_generated = [dt.datetime.strftime(start + dt.timedelta(days=x),'%d-%m-%Y') for x in range(0, (end-start).days)] 
URL = {'path':'https://covid19.karnataka.gov.in/storage/pdf-files/Media-Bulletin/', 'extension':' HMB English.pdf'}

def download_pdf(pdf):
    path, url = pdf
    if not os.path.exists(path):
        r = requests.get(url,stream=True) 
        if r.status_code == 200:
            with open(path,'wb') as f:
                for chunk in r:
                    f.write(chunk)
    return path

def make_paths_and_urls():
    global URL,dates_generated 
    base_path = 'data/06-07/'
    paths = [] 
    for date in dates_generated:
        paths.append((base_path + date + '.pdf', URL['path'] + date + URL['extension']))
    return paths 

def main():
    pdfs = make_paths_and_urls() 
    results = ThreadPool(8).imap_unordered(download_pdf, pdfs)
    for pdf in results:
        print(f'{pdf} downloaded')


if __name__ == "__main__":
    main()