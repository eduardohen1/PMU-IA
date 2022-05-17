from importlib.resources import path
import pandas as pd
import os
import glob
import time
import pymongo
import json

oneDrive = "C:/Unifei/PMU/PMU-IA/"
eventos = ['evento 1/*.zip']

mngClient = pymongo.MongoClient('mongodb://localhost:27017/')
mngDb = mngClient['local'] #mngClient.local
collectionName = 'pmu'

dbCon = mngDb[collectionName]


for evento in eventos:
    files = glob.glob(os.path.join(oneDrive,evento))
    total = files.count
    contar = 1
    inic = time.time()
    dfs = []
    for file in files:
        print('>> ' + str(contar)  + ' - ' + str(file))        
        for chunk in pd.read_csv(file, chunksize=1000, skiprows=1):            
            #with open('saida.json','w') as file:
            #    chunk.to_json(file)            
            dataJson = chunk.to_json()
            vlrInsert = json.loads(dataJson);            
            dbCon.insert_one(vlrInsert)
            contar += 1
            break
fim = time.time()
print('..Tempo: ' + str(fim-inic)) 
mngClient.close()