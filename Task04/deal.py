import pandas as pd


path = "/home/hadoop/taidibei.csv"
df = pd.read_csv(path)
df_temp = df.loc[:,'竞赛':'农业']
# print(df_temp)
flag_list = []
for i in range(len(df_temp)-1):
    if df_temp.loc[i].sum() == 0:
        flag_list.append(i)
df.drop(labels=flag_list, axis=0,inplace = True)
df.to_csv('/home/hadoop/taidibei_filter.csv',index=False)
