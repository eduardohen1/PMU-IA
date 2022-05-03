# SuperFastPython.com
# unzip a large number of files concurrently with processes and threads in batch
from os import makedirs
from os.path import join
from zipfile import ZipFile
import os
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
 
# save file to disk
def save_file(data, filename, path):
    # create a path
    filepath = join(path, filename)
    # write to disk
    with open(filepath, 'wb') as file:
        file.write(data)
    # report progress
    print(f'.unzipped {filename}', flush=True)
 
# unzip files from an archive
def unzip_files(zip_filename, filenames, path):
    # open the zip file
    with ZipFile(zip_filename, 'r') as handle:
        # create a thread pool
        with ThreadPoolExecutor(10) as exe:
            # unzip each file
            for filename in filenames:
                # decompress data
                data = handle.read(filename)
                # save to disk
                _ = exe.submit(save_file, data, filename, path)
 
# unzip a large number of files
def main(path='.'):    
    for diretorio, subpastas, arquivos in os.walk(path):
        for arquivo in arquivos:
            if(arquivo.endswith('.zip')):
                inic = time.time()
                zip_filename = arquivo
    
                # create the target directory
                makedirs(path, exist_ok=True)
                # open the zip file
                with ZipFile(zip_filename, 'r') as handle:
                    # list of all files to unzip
                    files = handle.namelist()
                # determine chunksize
                n_workers = 1
                chunksize = round(len(files) / n_workers)
                # start the thread pool
                with ProcessPoolExecutor(n_workers) as exe:
                    # split the copy operations into chunks
                    for i in range(0, len(files), chunksize):
                        # select a chunk of filenames
                        filenames = files[i:(i + chunksize)]
                        # submit the batch copy task
                        _ = exe.submit(unzip_files, zip_filename, filenames, path)
                fim = time.time()
                print('..Tempo: ' + str(fim-inic))  
# entry point
if __name__ == '__main__':
    print('................ PROG 02')
    inic_proj = time.time()
    main()
    print('Tempo total: ' + str(time.time() - inic_proj))