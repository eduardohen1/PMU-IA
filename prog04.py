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

mngClient = pymongo.MongoClient('mongodb://localhost:27017/')
mngDb = mngClient['local'] #mngClient.local
mngPMU = mngDb["pmu"]

pmu = mngPMU.find({"pmu1" : {"$in" : ['C37118-1000', 'C37118-1001','C37118-1003']}})
pmu = pd.DataFrame(pmu)
pmuPivot = pd.pivot_table(pmu, index=['DataHora2'], fill_value=0)

x = pmuPivot.loc[:, pmuPivot.columns.values.tolist()].values
y = list(pmuPivot.index)

plt.figure(figsize=(9,9),cmap = 'viridis_r', annot=True)
sns.heatmap(pmuPivot.data[:,pmuPivot.columns.values.tolist()].corr(), cmap='viridis_r', annot=True)
plt.tight_layout()
plt.show()
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