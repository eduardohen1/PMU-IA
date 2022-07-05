import pandas as pd
import time
import pymongo
import json
import numpy as np

# Buscar dados de 3 PMUs:

# -------------------------------------
# Conexão com o MongoDB:
mngClient = pymongo.MongoClient('mongodb://localhost:27017/')
mngDb = mngClient['local'] #mngClient.local
mngPMU = mngDb["pmu"]
dbCon = mngDb["pmuDiv"]

'''
# exportando de DB para csv:
pmuDiv = dbCon.find({})
pmuDiv = pd.DataFrame(pmuDiv)
pmuDiv.to_csv("/home/eduardo/Documentos/UNIFEI/pmuDiv.csv")
'''

pmu = mngPMU.find({"pmu1" : {"$in" : ['C37118-1000', 'C37118-1001','C37118-1003']}})
pmu = pd.DataFrame(pmu)

# -------------------------------------

columnsNewDF = ['id', 'index', 'Date', 'Time (America/Sao_Paulo)', 
                'Status', 'Frequency', 'df/dt', 'pmu1', 'DataHora2', 
                'Tipo1', 'Tipo2', 'Tipo3', 'Angle', 'Magnitude']

colunasPadrao = ['_id', 'index', 'Date', 'Time (America/Sao_Paulo)', 
                 'Status', 'Frequency', 'df/dt', 'pmu1', 'DataHora2', 
                 'Tipo1', 'Tipo2', 'Tipo3', 'Angle', 'Magnitude', 'pmu2', 'dataRefPMUIni',
                 'dataRefPMUFim', 'nomeArquivo', 'DataHora']

iteracao = 0
indexLine = 0
pmuNew = pd.DataFrame(columns=columnsNewDF)
for index, row in pmu.iterrows():
    #Zerando variáveis a cada iteração
    interaCol       = 0   
    newRow          = []
    newRowInst      = []
    primaParte      = []
    newRowCol       = []
    newRowColInsert = []
    primaParteCol   = []    
    #loop nas colunas:
    for coluna in pmu.columns:        
        # verificação se a 'coluna' está na lista de colunas padrão
        if not (coluna in colunasPadrao):
            # verificação se o valor da coluna é null, caso sim, próximo
            if not (pd.isnull(row[coluna])):                
                txtColuna  = coluna.split(" ")                
                txtColuna2 = txtColuna[1].split("_")
                
                if(interaCol == 0):                
                    newRowCol.append("Tipo1")
                    newRow.append(txtColuna[0])

                    newRowCol.append("Tipo2")
                    newRow.append(txtColuna2[0] + "_" + txtColuna2[1])
                    
                    if(len(txtColuna2) > 2):
                        newRowCol.append("Tipo3")
                        newRow.append(txtColuna2[2])

                if(txtColuna[2].upper().__eq__('ANGLE')):
                    newRowCol.append('Angle')
                    newRow.append(row[coluna])
                    interaCol += 1            
                if(txtColuna[2].upper().__eq__('MAGNITUDE')):
                    newRowCol.append('Magnitude')
                    newRow.append(row[coluna])
                    interaCol += 1
        else:            
            if not (coluna.upper().__eq__('_ID')):
                primaParteCol.append(coluna)
                primaParte.append(row[coluna])            
        if(interaCol == 2):
            #Verificando se já existe os campos de DataHora2 e pmu1 nos arrays:
            existData = False
            existPmu  = False
            for c in newRowCol:
                if (c.rstrip().lstrip().upper().__eq__('DATAHORA2')):
                    existData = True
                if (c.rstrip().lstrip().upper().__eq__('PMU1')):
                    existPmu = True

            if (not existData) and (not existPmu):
                for c in primaParteCol:
                    if (c.rstrip().lstrip().upper().__eq__('DATAHORA2')):
                        existData = True
                    if (c.rstrip().lstrip().upper().__eq__('PMU1')):
                        existPmu = True
            
            #Caso não existir, acrescentar os campos DataHora2 e pmu1:
            if not (existData):
                newRow.append(row['DataHora2'])
                newRowCol.append('DataHora2')
            
            if not (existPmu):
                newRow.append(row['pmu1'])
                newRowCol.append('pmu1')

            #Criando campo primary_key:
            indexLine += 1
            newRowCol.append('id')
            newRow.append(indexLine)

            #Concatenando a lista da 1a parte e a lista da segunda parte de campos:
            newRowInst      = np.concatenate((primaParte, newRow))
            newRowColInsert = np.concatenate((primaParteCol, newRowCol))    

            #Inserindo no DataFrame novo
            pmuColumn = pd.DataFrame([newRowInst], columns=newRowColInsert)
            pmuNew = pd.concat([pmuNew, pmuColumn], ignore_index=True, axis=0)
            interaCol       = 0
            newRow          = []
            newRowInst      = []
            newRowCol       = []
            newRowColInsert = []

    #Inserindo registro no banco de dados // pode substituir por arquivo:
    iteracao += 1
    print('>>>> ' + str(iteracao) + '/' + str(pmu.count()[0]))
    pmuNew.set_index('id')    
    dataJson = pmuNew.to_json(orient="table") 
    vlrPreInsert = json.loads(dataJson);            
    vlrInsert = vlrPreInsert['data']            
    #dbCon.insert_one(vlrInsert)
    dbCon.insert_many(vlrInsert)
    pmuNew = pd.DataFrame(columns=columnsNewDF)

mngClient.close()       
'''
for index, row in pmu.iterrows():  
    row.  
    dados = [row['index'], row['Date'], row['Time (America/Sao_Paulo)'], row['Status'], row['Frequency'],
             row['df/dt'], row['pmu1'], row['DataHora2']
             ]   
    print(dados) 
'''
x= 1


