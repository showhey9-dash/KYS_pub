
# coding: utf-8

# In[1]:

# 特長量を知るためにデータごとの相関関係を求める
import pandas as pd
import numpy as np
from pandas.tools.plotting import scatter_matrix
import matplotlib.pyplot as plt
import os


# In[2]:

file1 = pd.read_csv('csv\モデルD用_20181103-1104.csv',  engine='python', encoding='cp932')


# In[3]:

# 欠損行を削除
file1 = file1.dropna(how='any')
print(len(file1))


# In[21]:

from pandas.tools.plotting import scatter_matrix
plt.figure()
scatter_matrix(file1)
plt.show()


# In[8]:

file1.describe()


# In[22]:

#file1['枠コース']


# In[31]:

# 散布図
plt.scatter(file1['枠騎手'],file1['枠調教師'])
plt.show()

#相関係数
np.corrcoef(file1['枠騎手'],file1['枠調教師'])


# In[ ]:



