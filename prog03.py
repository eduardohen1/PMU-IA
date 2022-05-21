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
        #pegando apenas nome do arquivo, sem o path
        fileName_absolute = os.path.basename(file)    
        print('>> ' + str(contar)  + ' - ' + fileName_absolute)
        arquivo = fileName_absolute.split('-')
        for chunk in pd.read_csv(file, chunksize=1000, skiprows=1): 
            #adicionando novas colunas com o nome do arquivo, datas e outros dados
            chunk['pmu1'] = arquivo[0] + '-' + arquivo[1]
            chunk['pmu2'] = arquivo[2]
            chunk['dataRefPMUIni'] = arquivo[3]
            chunk['dataRefPMUFim'] = arquivo[4][:-4]
            chunk['nomeArquivo'] = str(fileName_absolute)
            #orientação em table para gerar os dados tipo objeto tabular, sem este o to_json gera um objeto por coluna contendo todas as linhas
            dataJson = chunk.to_json(orient="table") 
            vlrPreInsert = json.loads(dataJson);            
            vlrInsert = vlrPreInsert['data']            
            #dbCon.insert_one(vlrInsert)
            dbCon.insert_many(vlrInsert)
            contar += 1
            break
fim = time.time()
print('..Tempo: ' + str(fim-inic)) 
mngClient.close()