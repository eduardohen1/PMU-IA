# Tentativa de extração via cores:
# https://www.peterbe.com/plog/fastest-way-to-unzip-a-zip-file-in-python
import os
import zipfile
import concurrent.futures
import time

def _count_file(fn):
    with open(fn, 'rb') as f:
        return _count_file_object(f)

def _count_file_object(f):
    total = 0
    for line in f:
        total += len(line)
    return total

def unzip_member_f3(zip_filepath, filename, dest):
    with open(zip_filepath, 'rb') as f:
        zf = zipfile.ZipFile(f)
        zf.extract(filename, dest)
    fn = os.path.join(dest, filename)
    return _count_file(fn)

def f3(fn, dest):
    with open(fn, 'rb') as f:
        zf = zipfile.ZipFile(f)
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for member in zf.infolist():
                futures.append(
                    executor.submit(
                        unzip_member_f3,
                        fn,
                        member.filename,
                        dest,
                    )
                )                
            total = 0
            for future in concurrent.futures.as_completed(futures):
                total += future.result()
    return total


path = '.'
files = []
print('................ PROG 01')
inic_proj = time.time()
for diretorio, subpastas, arquivos in os.walk(path):
    for arquivo in arquivos:
        if(arquivo.endswith('.zip')):
            #print(arquivo.replace(".zip",".txt") + '\n')
            #files.append(os.path.join(diretorio, arquivo))
            inic = time.time()
            print('.unzipped ' + arquivo)
            f3(arquivo, ".")
            fim = time.time()
            print('..Tempo: ' + str(fim-inic)) 
print('Tempo total: ' + str(time.time() - inic_proj))
