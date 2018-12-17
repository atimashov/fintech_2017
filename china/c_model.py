# -*- coding: utf-8 -*-
"""
Created on Wed May 17 16:01:37 2017

@author: aleks
"""
import numpy as np
import pandas as pd
import math
import time
import sqlalchemy
import psycopg2
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import CHAR
from sqlalchemy.types import ARRAY

"""LOAD DATA"""
time_help=time.time()
dataTarget=getTableFromMyDB('target',schema='general',req='"GATEID","SIGNDT","EXPDT","TARGET"',where='"SIGNDT" IS NOT NULL')
dataTarget.index=dataTarget['GATEID']
uids="uid IN ('"+"','".join(list(dataTarget['GATEID']))+"')"
data = getTableFromMyDB('main_table',schema='general',where=uids)
data.index=data['uid']
data['TARGET']=dataTarget['TARGET']
data.sort_values(by='created_at',inplace=True,ascending=True)
data.index=list(range(data.shape[0]))
data['YW']=list(map(lambda x:x.isocalendar()[0]*100+x.isocalendar()[1],data['created_at']))
data['YM']=list(map(lambda x:x.isocalendar()[0]*100+x.month,data['created_at']))
del uids, dataTarget

y = data['TARGET']
data.drop('TARGET',1,inplace=True)
train_test_YW=201651
nBins=3
bigGroup=50
"""GET INFO_DF"""
"""Обязательно иметь переменные YW и YM, являющиеся производными от даты выдачи"""
""""""
info_df=getInfo(data,train_test_YW,minInGroup=bigGroup,n_notForBin=nBins)
info_df.loc[(info_df['col_name']=='YM') | (info_df['col_name']=='YW'),'group']='help'
#******************
#отмечаем дубликаты (более 90% из непустых совпадает, в комментарий копия от чего и %)
#******************
delete_copy=[('notForBin','notForBin'),('location','location'),('phones','phones'),('nominal','nominal'),('numeric','numeric'),('na','na'),
             ('code','code'),('ids','ids'),('datetimes','datetimes'),
             ('location','nominal'),('location','numeric'),('location','ids'),('location','na'),('location','code'),
             ('nominal','numeric'),('nominal','ids'),('nominal','code'),('na','numeric'),('na','ids'),('na','code'),('ids','code'),
             ('ids','numeric'),('code','numeric')]
             
for i in range(len(delete_copy)):
    vars_delete=getCols(*delete_copy[i],info_df=info_df)
    if len(vars_delete)>0:
        for var_comp in vars_delete:
            comp=bool_copy_percent(data[var_comp[0]],data[var_comp[1]])

            if comp!='different':                
                if comp[0]==0:
                    group=info_df.loc[info_df['col_name']==var_comp[1],'group'].values[0]
                    info_df.loc[info_df['col_name']==var_comp[1],'comment']='copy:'+comp[1]+group+' '+var_comp[0]    
                    info_df.loc[info_df['col_name']==var_comp[1],'group']='copy'                                  
                elif comp[0]==1:
                    group=info_df.loc[info_df['col_name']==var_comp[1],'group'].values[0]
                    info_df.loc[info_df['col_name']==var_comp[1],'comment']='main:'+comp[1]+group+' '+var_comp[0]    
                    info_df.loc[info_df['col_name']==var_comp[1],'group']='copy'
                    data[var_comp[0]]=data[var_comp[1]]                    
                elif comp[0]==2:
                    print ('2:',var_comp,' ',comp[1])
#*****************
print ('preface:',round(time.time()-time_help,2))
del delete_copy,i,vars_delete,var_comp,comp, group,time_help
"""Изменяем значения вручную"""
cond=(info_df['null_test']<90) & (info_df['null_train']<90) & (info_df.not_null_finish>=train_test_YW+3) & (info_df.num_of_unique_test>=2)
info_df.loc[(info_df.group=='notForBin') & cond  ,'group_sup']='fastWOE'
info_df.loc[list(map(lambda x:x in set(['amount','salary','amount_wish','term']),info_df.col_name)) & cond  ,'group_sup']='numeric'
#***********
cond_nominal=(info_df.cnt_big_groups>=nBins+1) & (info_df.cnt_big_groups<100) & (info_df.null_test<90) & (info_df.null_train<90) & (info_df.group_sup=='') & (list(map(lambda x: x in set(['numeric','location','nominal','ids','code']),info_df.group)))
info_df.loc[cond_nominal,'group_sup']='nominal'            
del cond, cond_nominal
"""делим выборку на train/test"""
data_train,y_train = (data.loc[data.YW<=train_test_YW, info_df.col_name[info_df.group_sup!=''].values], y[data.YW<=train_test_YW])
data_test,y_test = (data.loc[data.YW>train_test_YW, info_df.col_name[info_df.group_sup!=''].values], y[data.YW>train_test_YW])
#меняем значения null на 'null'
for variable in info_df.loc[(info_df.group_sup=='fastWOE') | (info_df.group_sup=='nominal'),'col_name']:
    data_train.loc[data_train[variable].isnull(),variable]='null'
    data_train[variable]=data_train[variable].astype(object)
    data_test.loc[data_test[variable].isnull(),variable]='null'
    data_test[variable]=data_test[variable].astype(object)

"""Count WOE"""
flag_statistic=0
#group='fastWOE'
time_help=time.time()
for variable in info_df.loc[info_df.group_sup=='fastWOE','col_name']: 
    print (variable)
    df_step,pivot_step=woeFast(data_train[variable],y_train,minInGroup=bigGroup)
    info_df.loc[info_df['col_name']==variable,'IV']=pivot_step['IV'].sum()
    df_step['variable']=variable
    pivot_step['variable']=variable
    
    df_step['bottomBorder']=df_step.bottomBorder.astype('object')
    df_step['topBorder']=df_step.topBorder.astype('object')
    if flag_statistic==0:
        df_stat=df_step
        pivot_stat=pivot_step
        flag_statistic=1
    else:
        df_stat=df_stat.append(df_step,ignore_index=True)    
        pivot_stat=pivot_stat.append(pivot_step,ignore_index=True)    
print ('fastWOE:',round(time.time()-time_help,2))
print('________________________')
print()    
#group='numeric'
time_help=time.time()
for variable in info_df.loc[info_df.group_sup=='numeric','col_name']:
    print(variable)
    df_step,pivot_step=woeNumeric(data_train[variable],y_train,n_bins=nBins,n_original=50,min_in_group=bigGroup)
    info_df.loc[info_df.col_name==variable,'IV']=pivot_step.IV.sum()
    df_step['variable']=variable
    pivot_step['variable']=variable
    
    df_step['bottomBorder']=df_step.bottomBorder.astype('object')
    df_step['topBorder']=df_step.topBorder.astype('object')
    if flag_statistic==0:
        df_stat=df_step
        pivot_stat=pivot_step
        flag_statistic=1    
    else:
        df_stat=df_stat.append(df_step,ignore_index=True)    
        pivot_stat=pivot_stat.append(pivot_step,ignore_index=True)
print ('numeric:',round(time.time()-time_help,2))
print('________________________')
print()
#group='nominal'
time_help=time.time()
for variable in info_df.loc[info_df.group_sup=='nominal','col_name']:
    print(variable)
    df_step,pivot_step=woeNominal(data_train[variable],y_train,n_bins=nBins,min_in_group=bigGroup)
    info_df.loc[info_df['col_name']==variable,'IV']=pivot_step['IV'].sum()
    df_step['variable']=variable
    pivot_step['variable']=variable
    
    df_step['bottomBorder']=df_step.bottomBorder.astype('object')
    df_step['topBorder']=df_step.topBorder.astype('object')
    if flag_statistic==0:
        df_stat=df_step
        pivot_stat=pivot_step
        flag_statistic=1
    else:
        df_stat=df_stat.append(df_step,ignore_index=True)    
        pivot_stat=pivot_stat.append(pivot_step,ignore_index=True)     
print ('nominal:',round(time.time()-time_help,2))
del df_step, flag_statistic,pivot_step,time_help,variable

"""construct set for forecast"""
woe_train=pd.DataFrame(index=y_train.index)
woe_test=pd.DataFrame(index=y_test.index)
#fast
time_help=time.time()
for variable in df_stat.loc[df_stat['typeOfGroup']=='woeFast','variable'].unique():
    train,test=colToWoe(data_train[variable],data_test[variable],df_stat.loc[df_stat['variable']==variable,['bottomBorder','woe']],'woeFast')
    woe_train.insert(0,variable,train)
    woe_test.insert(0,variable,test)
#numeric
for variable in df_stat.loc[df_stat['typeOfGroup']=='woeNumeric','variable'].unique():
    train,test=colToWoe(data_train[variable],data_test[variable],df_stat.loc[df_stat['variable']==variable,['bottomBorder','topBorder','woe']],'woeNumeric')
    woe_train.insert(0,variable,train)
    woe_test.insert(0,variable,test)
#nominal
for variable in df_stat.loc[(df_stat['typeOfGroup']=='woeNominal') | (df_stat['typeOfGroup']=='nominalFast'),'variable'].unique():
    train,test=colToWoe(data_train[variable],data_test[variable],df_stat.loc[df_stat['variable']==variable,['bottomBorder','woe']],'woeNominal')
 #   train,test=colToWoe(data_train[woe_nominal[0][i]],data_test[woe_nominal[0][i]],woe_nominal[1][i],'nominal')
    woe_train.insert(0,variable,train)
    woe_test.insert(0,variable,test)
print ('get woe:',round(time.time()-time_help,2))
del time_help, variable

"""Модель"""
from sklearn.linear_model import LogisticRegression
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import roc_auc_score

param_grid = {'C': [0.01, 0.05, 0.1, 0.2, 0.3, 0.35, 0.4, 0.5, 0.7, 1, 5, 10]}
optimizer = GridSearchCV(LogisticRegression('l2'), param_grid,cv=5)

"""все переменные"""
optimizer.fit(woe_train,y_train)
pred_train=optimizer.predict_proba(woe_train)
pred_test=optimizer.predict_proba(woe_test)

gini_train=round(100.*(2*roc_auc_score(y_train,pred_train[:,1:])-1),2)
gini_test=round(100.*(2*roc_auc_score(y_test,pred_test[:,1:])-1),2)
print ('each variable: gini_train=',gini_train,', gini_test=',gini_test)

"""без номинальных"""                 
cols=info_df.loc[(info_df.group_sup=='numeric') | (info_df.group_sup=='fastWOE'),'col_name'].values
optimizer.fit(woe_train[cols],y_train)
pred_train=optimizer.predict_proba(woe_train[cols])
pred_test=optimizer.predict_proba(woe_test[cols])

gini_train=round(100.*(2*roc_auc_score(y_train,pred_train[:,1:])-1),2)
gini_test=round(100.*(2*roc_auc_score(y_test,pred_test[:,1:])-1),2)
print ('without nominal: gini_train=',gini_train,', gini_test=',gini_test)

"""IV>0.01"""                 
cols=info_df.loc[info_df.IV>0.01,'col_name'].values
optimizer.fit(woe_train[cols],y_train)
pred_train=optimizer.predict_proba(woe_train[cols])
pred_test=optimizer.predict_proba(woe_test[cols])

gini_train=round(100.*(2*roc_auc_score(y_train,pred_train[:,1:])-1),2)
gini_test=round(100.*(2*roc_auc_score(y_test,pred_test[:,1:])-1),2)
print ('IV greater than 0.01: gini_train=',gini_train,', gini_test=',gini_test)