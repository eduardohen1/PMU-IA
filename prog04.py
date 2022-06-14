#sklearning
#incremental pca1
#parcial fit
#	parciais (hrs parciais).
#================
#parciais
#	transform -> converte para duas
#		tirar média (x,y) de 10 em 10 min = 1 ponto; cor do ponto associado ao hr
#		desvio padrão
#matplot
#https://scikit-learn.org/stable/computing/scaling_strategies.html
#{  "_id": {    "$oid": "628c45abb51b6014c8c8a0d6"  },  "index": 0,  "Date": "08/04/21",  "Time (America/Sao_Paulo)": "18:57:32.150",  "Status": "02 00",  "Frequency": 60.026939,  "df/dt": 0.0031,  "VA1 V_BARRA_230k Magnitude": 240226.84375,  "VA1 V_BARRA_230k Angle": -76.970764,  "VB1 V_BARRA_230k Magnitude": 240519.265625,  "VB1 V_BARRA_230k Angle": 163.115448,  "VC1 V_BARRA_230k Magnitude": 239828.953125,  "VC1 V_BARRA_230k Angle": 42.841148,  "IA1 I_BTA_230kV Magnitude": 219.873993,  "IA1 I_BTA_230kV Angle": 99.238098,  "IB1 I_BTA_230kV Magnitude": 208.874649,  "IB1 I_BTA_230kV Angle": -23.539661,  "IC1 I_BTA_230kV Magnitude": 209.853027,  "IC1 I_BTA_230kV Angle": -140.895279,  "IA1 I_CCO_230kV Magnitude": 94.489861,  "IA1 I_CCO_230kV Angle": 86.591942,  "IB1 I_CCO_230kV Magnitude": 106.242867,  "IB1 I_CCO_230kV Angle": -34.531178,  "IC1 I_CCO_230kV Magnitude": 99.143173,  "IC1 I_CCO_230kV Angle": -156.500992,  "IA1 I_SMC_230kV Magnitude": 108.187889,  "IA1 I_SMC_230kV Angle": -96.671799,  "IB1 I_SMC_230kV Magnitude": 98.440918,  "IB1 I_SMC_230kV Angle": 138.300537,  "IC1 I_SMC_230kV Magnitude": 101.352234,  "IC1 I_SMC_230kV Angle": 27.493616,  "IA1 I_CTN_230kV Magnitude": 42.845097,  "IA1 I_CTN_230kV Angle": 72.586006,  "IB1 I_CTN_230kV Magnitude": 27.785915,  "IB1 I_CTN_230kV Angle": -33.962856,  "IC1 I_CTN_230kV Magnitude": 40.460487,  "IC1 I_CTN_230kV Angle": -142.428192,  "pmu1": "C37118-1070",  "pmu2": "PIL_230kV",  "dataRefPMUIni": "20210408185732",  "dataRefPMUFim": "20210408235859",  "nomeArquivo": "C37118-1070-PIL_230kV-20210408185732-20210408235859.zip",  "DataHora": "2021-04-08T18:57:32.150Z",  "DataHora2": "20210408_185732.150000"}
#transformar tabela em Pivot...
#Data/Hora | Frequencia PMU1 | 
from cProfile import label
import pandas as pd
import os
import glob
import time
import pymongo
import json
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA, IncrementalPCA
import seaborn as sns
from yaml import load

mngClient = pymongo.MongoClient('mongodb://localhost:27017/')
mngDb = mngClient['local'] #mngClient.local
mngPMU = mngDb["pmu"]

pmu = mngPMU.find({"pmu1" : {"$in" : ['C37118-1000', 'C37118-1001','C37118-1003']}})
pmu = pd.DataFrame(pmu)

pmuPivot = pmu.pivot_table(index="DataHora2", columns="pmu1", fill_value=0)
pmuPivotNormalize = ((pmuPivot - pmuPivot.mean()) / pmuPivot.std()).fillna(0)
print(pmuPivotNormalize)

colunas = pmuPivot.columns.values
y = pmuPivot.columns.T.codes

X = pmuPivot
n_components = pmuPivot.shape[1]
ipca = IncrementalPCA(n_components=n_components)
X_ipca = ipca.fit_transform(X)

pca = PCA(n_components=n_components)
X_pca = pca.fit_transform(X)

colors = ["navy", "turquoise", "darkorange"]

for X_transformed, title in [(X_ipca, "Incremental PCA"), (X_pca, "PCA")]:
    plt.figure(figsize=(8, 8))
    for color, i, target_name in zip(colors, [0, 1, 2], colunas):
        plt.scatter(
            X_transformed[y == i, 0],
            X_transformed[y == i, 1],
            color=color,
            lw=2,
        )

    if "Incremental" in title:
        err = np.abs(np.abs(X_pca) - np.abs(X_ipca)).mean()
        plt.title(title + " of dataset\nMean absolute unsigned error %.6f" % err)
    else:
        plt.title(title + " of dataset")
    plt.legend(loc="best", shadow=False, scatterpoints=1)
    #plt.axis([-4, 4, -1.5, 1.5])
plt.show()


#pca = PCA(n_components=pmuPivot.shape[1])
#pca.fit(pmuPivotNormalize)
#loadings = pd.DataFrame(pca.components_.T, 
#                        columns=['PC%s' % _ for _ in range(len(pmuPivotNormalize.columns))],
#                        index=pmuPivot.columns)
#
#plt.plot(pca.explained_variance_ratio_)
#plt.ylabel('yLabel')
#plt.xlabel('xLabel')
#plt.show()


#pmuPivot = pd.pivot_table(pmu, index=['DataHora2'], fill_value=0)

#x = pmuPivot.loc[:, pmuPivot.columns.values.tolist()].values
#y = list(pmuPivot.index)

#plt.figure(figsize=(9,9),cmap = 'viridis_r', annot=True)
#sns.heatmap(pmuPivot.data[:,pmuPivot.columns.values.tolist()].corr(), cmap='viridis_r', annot=True)
#plt.tight_layout()
#plt.show()
#x = pmuPivot.dropna(how='all')
#y = pmuPivot.columns.values.tolist()#pmuPivot[pmuPivot.columns]
#n_componets = 2
#ipca = IncrementalPCA(n_components=n_componets, batch_size=10)
#X_ipca = ipca.fit_transform(x)
##scatter_plot(X_ipca, pmuPivot.columns.values.tolist())
#pca = PCA(n_components=n_componets)
#X_pca = pca.fit_transform(x)

#colors = ["navy", "turquoise", "darkorange"]

#for X_transformed, title in [(X_ipca, "IncrementalPCA"), (X_pca, "PCA")]:
#    plt.figure(figsize=(20,20))
#    for color, i, target_name in zip(colors, [0,1,2],pmuPivot.columns.values.tolist()):
#        plt.scatter(
#            X_transformed[y == i, 0],
#            X_transformed[y == i, 1],
#            color = color,
#            lw=2,
#            label = pmuPivot.columns.values.tolist(),
#        )
#    if "Incremental" in title:
#        err = np.abs(np.abs(X_pca) - np.abs(X_ipca)).mean()
#        plt.title(title + " erro: %.6f" % err)
#    else:
#        plt.title(title + " da base PMU")
#    plt.legend(loc="best", shadow=False, scatterpoints=1)
#    #plt.axis([4,-4,-1.5,1.5])
#plt.show()
##print(pmuPivot)
x=1