import pandas as pd
import time
import pymongo
import json

# Buscar dados de 3 PMUs:
# Ver os campos de cada documento:
'''
Serão observados os campos, retirando o 
    _id, index, Date, Time (America/Sao_Paulo), Status, Frenquency, df/dt,
    pmu1, pmu2, dataRefPMUini, dataRefPMUFim, nomeArquivo, DAtaHora, DataHora2
'''
# Gerar uma tabela contendo:
'''
    index, Date, Time (America/SaoPaulo), Status, Frenquency, df/dt,
    pmu1, DataHora2,
    Tipo1, Tipo2, Tipo3, Angle, Magnitude:
    Ex.: (VA1 V_BARRA1_230 == Tipo1 VA1; Tipo2 V_BARRA1; Tipo3 230)
'''

mngClient = pymongo.MongoClient('mongodb://localhost:27017/')
mngDb = mngClient['local'] #mngClient.local
mngPMU = mngDb["pmu"]

pmu = mngPMU.find({"pmu1" : {"$in" : ['C37118-1000', 'C37118-1001','C37118-1003']}})
pmu = pd.DataFrame(pmu)

columnsNewDF = ['index', 'Date', 'Time (America/Sao_Paulo)', 
                'Status', 'Frequency', 'df/dt', 'pmu1', 'DataHora2', 
               'Tipo1', 'Tipo2', 'Tipo3', 'Angle', 'Magnitude']

dados = ['1','27/06/2022', '08:00', '02 00', 60.02, -0.004289, 'C37118-1000','20210408_060000.000000', 
         'VA1', 'V_BARRA1','230', 235609.765625,95.99189]

#pmuNew = pd.DataFrame.columns(columnsNewDF)
pmuNew = pd.DataFrame(columns=columnsNewDF)
    
'''
for index, row in pmu.iterrows():  
    row.  
    dados = [row['index'], row['Date'], row['Time (America/Sao_Paulo)'], row['Status'], row['Frequency'],
             row['df/dt'], row['pmu1'], row['DataHora2']
             ]   
    print(dados) 
'''
for row in pmu.itertuples():
    print(row.columns)
print(pmuNew)

x= 1


