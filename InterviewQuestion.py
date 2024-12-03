#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
#Read csv file 
df = pd.read_csv("/Users/prakashmahali/Desktop/Python_DataEngineer/data/weatherHistory.csv")


# In[2]:


#total number of elements in the DataFrame
df.size


# In[19]:


# To check rows and columns of dataframe 
df.shape


# In[20]:


rows,columns = df.shape
print(f"row :{rows} ,Columns : {columns}")


# In[21]:


# print Some sample records from top and buttoms
# default print 5 records
df.head()


# In[22]:


df.tail()


# In[23]:


#print row 2 to 5 - include 5
df[2:6]


# In[25]:


#print columns 
df.columns


# In[26]:


#print specific column value.
df['Summary'] #df.Summary


# In[27]:


df.Summary


# In[28]:


type(df.Summary)


# In[ ]:





# In[ ]:





# In[ ]:





# In[13]:


# create dataset using dictionary 
weather_data={
    'day':['1/1/2017','1/2/2017','1/3/2017','1/4/2017','1/5/2017','1/6/2017'],
    'temperature':[32,35,28,24,32,31],
    'windspeed':[6,7,2,7,4,2],
    'event':['Rain','Sunny','Snow','Snow','Rain','Sunny']
}
df1 = pd.DataFrame(weather_data)
df1


# In[12]:





# In[10]:


# Read json file 
df_json = pd.read_json("/Users/prakashmahali/Desktop/Python_DataEngineer/data/rdu-weather-history.json")
df_json


# In[15]:


get_ipython().system('pip install numpy')


# In[25]:


# Data Analytcs
import pandas as pd
df = pd.read_csv("/Users/prakashmahali/Desktop/Python_DataEngineer/data/DiwaliSalesData.csv",encoding= 'unicode_escape')
#ds.encode('utf-8').strip()
df.shape


# In[26]:


df.head()


# In[27]:


df.info()


# In[30]:


df.drop(['Status','unnamed1'],axis=1,inplace=True)


# In[29]:


df.head()


# In[32]:


df.info()


# In[34]:


pd.isnull(df).sum()


# In[36]:


df.shape


# In[37]:


df.dropna(inplace=True)


# In[38]:


df.shape


# In[39]:


pd.isnull(df).sum()


# In[3]:


#list omprehesion 
#List comprehension offers a shorter syntax when you want to create a new 
#list based on the values of an existing list.
#Example :
#Based on a list of fruits, you want a new list, containing only the fruits with the letter "a" in the name.
fruits = ["apple", "banana", "cherry", "kiwi", "mango"]
newlist = []

for x in fruits:
  if "a" in x:
    newlist.append(x)

print(newlist)


# In[ ]:


## Dict Compresion 
# Python allows dictionary comprehensions. We can create dictionaries using simple expressions.
#    A dictionary comprehension takes the form {key: value for (key, value) in iterable}




# In[5]:


## Pyspark Interview Question Capgemini
data=[
    (1,"prakash",45,"male",70),
    (2,"prakash1",46,"male",71),
    (3,"prakash2",47,"male",72),
    (4,"prakash3",48,"male",73)
]
schema="Id int,Name string,Age int ,Gender string,marks float"

df=spark.createDataFrame(data=data,schema=schema)
display(df)




# In[6]:



import math
import os
import random
import re
import sys



n=3
if n % 2 == 0 :
    if n >= 2 and n <= 5:
        print("Not Weird")
    elif n >= 6 and n <= 20:
        print("Weird")
    elif n > 20 : 
        print("Not Weird")
else :
    print("weird")


# In[15]:


n=3
My_list = [*range(1,n+1,1)] 
  
# Print the list 
print(*My_list,sep='')


# In[16]:


n=input()
print(n)


# In[18]:


#matplotlib
get_ipython().run_line_magic('pip', 'install matplotlib')


# In[9]:


#import matplotlib.pyplot as plt
get_ipython().system('pip install --upgrade pip')


# In[21]:


import matplotlib.pyplot as plt
plt.plot(1,2)


# In[17]:


import sys
print(sys.executable)


# In[1]:


nums = [1, 2, 3, 4, 2, 3]
unique_nums = list(set(nums))  # Convert to set and back to list
print(unique_nums)  # Output: [1, 2, 3, 4]


# In[3]:


list1 = [1, 2, 3, 4]
list2 = [3, 5, 6]

# Using set intersection
common = list(set(list1) & set(list2))
print(common)  # Output: {3}


# In[8]:


list1 = [1, 2, 3]
list2 = [3, 4, 5]

# Union
union = set(list1) | set(list2)
print(list(union) ) # Output: {1, 2, 3, 4, 5}

# Intersection
intersection = list(set(list1) & set(list2))
print(intersection)  # Output: {3}


# In[10]:


set1 = {1, 2, 3, 4}
set2 = {3, 4, 5}

# Difference
difference = set1 - set2
print(list(difference))  # Output: {1, 2}


# In[12]:


nums = [1, 2, 2, 3, 3, 3]

# Using a dictionary
freq = {}
for num in nums:
    freq[num] = freq.get(num, 0) + 1
print(freq)  # Output: {1: 1, 2: 2, 3: 3}


# In[13]:


set1 = {1, 2}
set2 = {1, 2, 3, 4}

if set1.issubset(set2):
    print("set1 is a subset of set2.")
else:
    print("set1 is not a subset of set2.")


# In[17]:


my_list = [1, 2, 3, 4, 2, 5, 1]
duplicates =set( [x for x in my_list if my_list.count(x) > 1])
print(duplicates)  # Output: {1, 2}


# In[15]:


my_list.count(2)


# In[ ]:




