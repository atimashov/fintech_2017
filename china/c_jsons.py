# -*- coding: utf-8 -*-
"""
Created on Wed May 17 16:02:41 2017

@author: aleks
"""
import pandas as pd
import numpy as np
import sqlalchemy
import math
import psycopg2
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import CHAR
from sqlalchemy.types import ARRAY
import time
#***************************
dfAppl=getTableFromDB(typeConnection='myDB', db='scorChina',showLogs=True)
infoJSON=getTree(dfAppl,True,True)

jsons=infoTypes.loc[infoTypes['types']==dict,'colName'].values

#коннект к вебреплике
try:
    server = SSHTunnelForwarder(('119.81.95.210', 22), ssh_password='Xy4n9kf9Yl0N', ssh_username='infinto', remote_bind_address=('127.0.0.1', 5432))
    server.start()                 
    conn_CN = psycopg2.connect(database='doctorcash_cn_production', user='infinto', password='Ozk1BPOjknTz', host='127.0.0.1' , port=server.local_bind_port)
    dfQGroup=pd.read_sql('select * from application_quangroup_answers' , conn_CN)
    a = 'подключение conn_CN  к локальной базе данных работает корректно на read'
except sshtunnel.BaseSSHTunnelForwarderError: 
    a = 'подключение conn_CN выдало ошибку'
    
    
#*******************************
engine = create_engine('postgresql://postgres:qmzpqm@localhost:5432/scorChina')
for i in range(math.ceil(dfAppl.shape[0]/10000)):
    time_help=time.time()
    #appl[(10000*i):(10000*(i+1))].to_sql('applications_jsonb',engine,index=False,dtype=dict.fromkeys(jsons,JSONB))
    dfAppl[(10000*i):(10000*(i+1))].to_sql('applications_1805',engine,index=False,dtype=dict.fromkeys(jsons,JSONB))    
    print(i+1,'-',math.ceil(dfAppl.shape[0]/10000),':',round(time.time()-time_help,2),' sec.')


keys=[]
for lists in temp:
    for lists_under in lists:
        keys.extend(list(lists_under.keys())) 
        keys=list(set(keys))
print (keys)


unique_values=[]
for key in keys:
    try:
        arr=[]
        for lists in temp:
            for dicts in lists:
                if key in dicts.keys():
                    arr.append(dicts[key])
                    arr=list(set(arr))
        unique_values.append((key,len(arr),arr))
    except:
        print('problem with:',key)
print (keys)


for lists in temp:
    if len(lists)==26:
        print(1)
        temp1=lists
    print (keys)
    
"""РАЗБИРАЕМ НА ТАБЛИЦЫ"""
InfoMain_general=Info.loc[(Info['depth']==1) & ((Info['partNotNA']==0) | ((Info['types']!=dict) & (Info['types']!=list)))]
InfoMain_lists=Info.loc[(Info['colName']=='id') | ((Info['depth']==1) & (Info['types']==list) & (Info['partNotNA']>0))]
#*************************
print (Info.loc[(Info['depth']==1) & ((Info['amUnique']==-2) | ((Info['types']==dict) & (Info['amUnique']>0))),['colName','types']])
#*************************
InfoTongdun_report=Info.loc[Info['mainKnot']=='tongdun_report_raw_response']
InfoTongdun_load=Info.loc[Info['mainKnot']=='tongdun_load_application_raw_response']

InfoMinshi_recognize=Info.loc[Info['mainKnot']=='response_minshi_recognize']
InfoMinshi_verification=Info.loc[Info['mainKnot']=='response_minshi_pair_verification']
InfoMinshi_police=Info.loc[Info['mainKnot']=='response_minshi_police_photo']

InfoUtm=Info.loc[Info['mainKnot']=='utm']
#*************************
InfoMain=InfoMain.append(InfoTongdun_load.loc[InfoTongdun_load['colName']=='tongdun_load_application_raw_response'],ignore_index=True)
#InfoLists1 - ok
InfoMinshi_verification=InfoMinshi_verification.append(Info.loc[Info['colName']=='id'],ignore_index=True)
#InfoMinshi_recognize - problem with
#InfoMinshi_police - problem with
InfoUtm=InfoUtm.append(Info.loc[Info['colName']=='id'],ignore_index=True)



from time import gmtime, strftime
strftime("%Y-%m-%d %H:%M:%S", gmtime())
'2009-01-05 22:14:39

time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


"""26 02"""
"""GENERAL"""
dfUTM=getTableFromMyDB('applications',req='Id, utm',showTime=True)
dfUTM=dfUTM.loc[(dfUTM['utm'].notnull()) & (dfUTM['utm']!=dict())]
temp=pd.DataFrame(dfUTM['utm'].tolist(),index=dfUTM.index)
temp.insert(0,'id',dfUTM['id'])
insertTableIntoMyDB(temp,'utm',schema='general')
del temp, dfUTM

dfStopFactors=getTableFromMyDB('applications',req='Id, stop_factors',showTime=True)
dfStopFactors=getDFfromLists(dfStopFactors.copy(),True)
insertTableIntoMyDB(dfStopFactors,'stop_factors',schema='general')
del dfStopFactors

dfSV_logged_in=getTableFromMyDB('applications',req='id, social_vector_logged_in',showTime=True)
dfSV_logged_in=getDFfromLists(dfSV_logged_in.copy(),True)
insertTableIntoMyDB(dfSV_logged_in,'social_vector_logged_in',schema='general')
del dfSV_logged_in

dfSV_Notlogged_in=getTableFromMyDB('applications',req='id, social_vector_not_logged_in',showTime=True)
dfSV_Notlogged_in=getDFfromLists(dfSV_Notlogged_in.copy(),True)
insertTableIntoMyDB(dfSV_Notlogged_in,'social_vector_not_logged_in',schema='general')
del dfSV_Notlogged_in

dfAppl=getTableFromMyDB('applications',showTime=True)
condDel=((infoAppl['types']=="<class 'dict'>") | (infoAppl['types']=="<class 'list'>")) & (infoAppl['depth']==1) 
colDel=list(infoAppl.loc[condDel,'colName'])
dfAppl.drop(colDel,axis=1,inplace=True)
insertTableIntoMyDB(dfAppl,'main_table',schema='general')
del dfAppl,condDel,colDel

"""MINSHI"""
dfMinshiPolice=getTableFromMyDB('applications',req='Id, response_minshi_police_photo',showTime=True)
dfMinshiPolice=dfMinshiPolice.loc[(dfMinshiPolice['response_minshi_police_photo'].notnull()) & (dfMinshiPolice['response_minshi_police_photo']!=dict())]
temp=pd.DataFrame(dfMinshiPolice['response_minshi_police_photo'].tolist(),index=dfMinshiPolice.index)
temp.insert(0,'id',dfMinshiPolice['id'])
insertTableIntoMyDB(temp,'police',schema='minshi')
del temp, dfMinshiPolice

dfMinshiRecognize=getTableFromMyDB('applications',req='Id, response_minshi_recognize',showTime=True)
dfMinshiRecognize=dfMinshiRecognize.loc[(dfMinshiRecognize['response_minshi_recognize'].notnull()) & (dfMinshiRecognize['response_minshi_recognize']!=dict())]
temp=pd.DataFrame(dfMinshiRecognize['response_minshi_recognize'].tolist(),index=dfMinshiRecognize.index)
temp.insert(0,'id',dfMinshiRecognize['id'])
insertTableIntoMyDB(temp,'recognize',schema='minshi')
del temp, dfMinshiRecognize

dfMinshiPair=getTableFromMyDB('applications',req='Id, response_minshi_pair_verification',showTime=True)
dfMinshiPair=dfMinshiPair.loc[(dfMinshiPair['response_minshi_pair_verification'].notnull()) & (dfMinshiPair['response_minshi_pair_verification']!=dict())]
temp=pd.DataFrame(dfMinshiPair['response_minshi_pair_verification'].tolist(),index=dfMinshiPair.index)
temp.insert(0,'id',dfMinshiPair['id'])
insertTableIntoMyDB(temp,'pair_verification',schema='minshi')
del temp, dfMinshiPair

"""TONGDUN"""
dfTongdun_errors=getTableFromMyDB('applications',req='id, tongdun_errors',showTime=True)
dfTongdun_errors=getDFfromLists(dfTongdun_errors.copy(),True)
insertTableIntoMyDB(dfTongdun_errors,'errors',schema='tongdun')
del dfTongdun_errors

dfTongdun_load=getTableFromMyDB('applications',req='id, tongdun_load_application_raw_response',showTime=True)
dfTongdun_load=dfTongdun_load.loc[(dfTongdun_load['tongdun_load_application_raw_response'].notnull()) & (dfTongdun_load['tongdun_load_application_raw_response']!=dict())]
temp=pd.DataFrame(dfTongdun_load['tongdun_load_application_raw_response'].tolist(),index=dfTongdun_load.index)
temp.insert(0,'id',dfTongdun_load['id'])
insertTableIntoMyDB(temp,'load_info',schema='tongdun')
del dfTongdun_load, temp
#tongdun_main
dfTongdun_report=getTableFromMyDB('applications',req='id, tongdun_report_raw_response',showTime=True)
dfTongdun_report=dfTongdun_report.loc[(dfTongdun_report['tongdun_report_raw_response'].notnull()) & (dfTongdun_report['tongdun_report_raw_response']!=dict())]
temp=pd.DataFrame(dfTongdun_report['tongdun_report_raw_response'].tolist(),index=dfTongdun_report.index)
temp.insert(0,'id',dfTongdun_report['id'])

dfTongdun_riskItems=temp[['id','risk_items']] #по хорошему, правильнее сюда report_id
temp.drop('risk_items',axis=1,inplace=True)
insertTableIntoMyDB(temp,'main_table',schema='tongdun')
del temp, dfTongdun_report

dfTongdun_riskItems=getDFfromLists(dfTongdun_riskItems.copy(),True)
temp=pd.DataFrame(dfTongdun_riskItems['risk_items'].tolist(),index=dfTongdun_riskItems.index)
temp.insert(0,'id',dfTongdun_riskItems['id'])
insertTableIntoMyDB(temp,'risk_items',schema='tongdun')
del temp,dfTongdun_riskItems

"""QUANTGROUP""" 
#ВАЖНО! Тут id - id запроса, id_application - id в applications
dfQG_error=getTableFromMyDB('quantgroup_all',req='id, error',showTime=True)
dfQG_error=getDFfromLists(dfQG_error.copy(),True)
insertTableIntoMyDB(dfQG_error,'error',schema='quantgroup')
del dfQG_error

colGeneral=','.join(list(infoQuant.loc[(infoQuant.types!="<class 'dict'>") & (infoQuant.types!="<class 'list'>") & (infoQuant.depth==1),'colName']))
dfQG_main=getTableFromMyDB('quantgroup_all',req=colGeneral,showTime=True)
insertTableIntoMyDB(dfQG_main,'main_table',schema='quantgroup')
del dfQG_main

dfQG_request=getTableFromMyDB('quantgroup_all',req='id, last_login_request',showTime=True)
dfQG_request=dfQG_request.loc[(dfQG_request['last_login_request'].notnull()) & (dfQG_request['last_login_request']!=dict())]
temp=pd.DataFrame(dfQG_request['last_login_request'].tolist(),index=dfQG_request.index)
temp.insert(0,'idMain',dfQG_request['id'])
insertTableIntoMyDB(temp,'last_login_request',schema='quantgroup')
del temp, dfQG_request
#JSON
dfQG_json=getTableFromMyDB('quantgroup_all',req='id, json',showTime=True)
dfQG_json=dfQG_json.loc[(dfQG_json['json'].notnull()) & (dfQG_json['json']!=dict())]
temp=pd.DataFrame(dfQG_json['json'].tolist(),index=dfQG_json.index)
temp.insert(0,'id',dfQG_json['id'])
dfQG_json=temp
del temp

dfReport_simple=dfQG_json[['id','agentName','modifyTime','remarkId','version']]
insertTableIntoMyDB(dfReport_simple,'report_simple',schema='quantgroup')
del dfReport_simple



dfBehaviorInfo=dfQG_json[['id','behaviorInfo']]
dfBehaviorInfo=dfBehaviorInfo.loc[(dfBehaviorInfo['behaviorInfo'].notnull()) & (dfBehaviorInfo['behaviorInfo']!=dict())]
insertTableIntoMyDB(dfBehaviorInfo,'behavior_info',schema='quantgroup')
del dfBehaviorInfo

dfСreditInfo=dfQG_json[['id','creditInfo']]
dfСreditInfo=dfСreditInfo.loc[(dfСreditInfo['creditInfo'].notnull()) & (dfСreditInfo['creditInfo']!=dict())]
insertTableIntoMyDB(dfСreditInfo,'creditInfo',schema='quantgroup')
del dfСreditInfo

dfFinancialInfo=dfQG_json[['id','financialInfo']]
dfFinancialInfo=dfFinancialInfo.loc[(dfFinancialInfo['financialInfo'].notnull()) & (dfFinancialInfo['financialInfo']!=dict())]
insertTableIntoMyDB(dfFinancialInfo,'financialInfo',schema='quantgroup')
del dfFinancialInfo

dfPersonalInfo=dfQG_json[['id','personalInfo']]
dfPersonalInfo=dfPersonalInfo.loc[(dfPersonalInfo['personalInfo'].notnull()) & (dfPersonalInfo['personalInfo']!=dict())]
insertTableIntoMyDB(dfPersonalInfo,'personalInfo',schema='quantgroup')
del dfPersonalInfo

dfPolicyInfo=dfQG_json[['id','policy']]
dfPolicyInfo=dfPolicyInfo.loc[(dfPolicyInfo['policy'].notnull()) & (dfPolicyInfo['policy']!=dict())]
insertTableIntoMyDB(dfPolicyInfo,'policy',schema='quantgroup')
del dfPolicyInfo

dfSocRelInfo=dfQG_json[['id','socialRelationship']]
dfSocRelInfo=dfSocRelInfo.loc[(dfSocRelInfo['socialRelationship'].notnull()) & (dfSocRelInfo['socialRelationship']!=dict())]
insertTableIntoMyDB(dfSocRelInfo,'socialRelationship',schema='quantgroup')
del dfSocRelInfo

dfTradeInfo=dfQG_json[['id','tradeInfo']]
dfTradeInfo=dfTradeInfo.loc[(dfTradeInfo['tradeInfo'].notnull()) & (dfTradeInfo['tradeInfo']!=dict())]
insertTableIntoMyDB(dfTradeInfo,'tradeInfo',schema='quantgroup')
del dfTradeInfo

"""!!!QUANT GROUP MY!!!"""
dfBehaviorInfo=dfQG_json[['id','behaviorInfo']]
dfBehaviorInfo=dfBehaviorInfo.loc[(dfBehaviorInfo['behaviorInfo'].notnull()) & (dfBehaviorInfo['behaviorInfo']!=dict())]
#*****************
dfСreditInfo=dfQG_json[['id','creditInfo']]
dfСreditInfo=dfСreditInfo.loc[(dfСreditInfo['creditInfo'].notnull()) & (dfСreditInfo['creditInfo']!=dict())]
#*****************
dfFinancialInfo=dfQG_json[['id','financialInfo']]
dfFinancialInfo=dfFinancialInfo.loc[(dfFinancialInfo['financialInfo'].notnull()) & (dfFinancialInfo['financialInfo']!=dict())]
#*****************
dfPersonalInfo=dfQG_json[['id','personalInfo']]
dfPersonalInfo=dfPersonalInfo.loc[(dfPersonalInfo['personalInfo'].notnull()) & (dfPersonalInfo['personalInfo']!=dict())]
#*****************
#*****************
dfTradeInfo=dfQG_json[['id','tradeInfo']]
dfTradeInfo=dfTradeInfo.loc[(dfTradeInfo['tradeInfo'].notnull()) & (dfTradeInfo['tradeInfo']!=dict())]
#*****************
del dfQG_json
#*****************

"""Social Relations"""
dfSocialRelations=dfQG_json[['id','socialRelationship']]
dfSocialRelations=dfSocialRelations.loc[(dfSocialRelations['socialRelationship'].notnull()) & (dfSocialRelations['socialRelationship']!=dict())]
#*****************
temp=pd.DataFrame(dfSocialRelations['socialRelationship'].tolist(),index=dfSocialRelations.index)
temp.insert(0,'reportId',dfSocialRelations['id'])
dfSocialRelations=temp
del temp
#lifeCircleAnalize
dfSR_lifeCircleAnalize=dfSocialRelations.loc[(dfSocialRelations['lifeCircleAnalize'].notnull()) & (dfSocialRelations['lifeCircleAnalize']!=dict()),['reportId','lifeCircleAnalize']]
temp=pd.DataFrame(dfSR_lifeCircleAnalize['lifeCircleAnalize'].tolist(),index=dfSR_lifeCircleAnalize.index)
temp.insert(0,'reportId',dfSR_lifeCircleAnalize['reportId'])
insertTableIntoMyDB(temp,'socialRelationship_lifeCircleAnalize',schema='quantgroup_my')
del temp, dfSR_lifeCircleAnalize
#messageList
dfSR_messageList=dfSocialRelations[['reportId','messageList']]
dfSR_messageList=getDFfromLists(dfSR_messageList.copy(),True)
temp=pd.DataFrame(dfSR_messageList['messageList'].tolist(),index=dfSR_messageList.index)
temp.insert(0,'reportId',dfSR_messageList['reportId'])
insertTableIntoMyDB(temp,'socialRelationship_messageList',schema='quantgroup_my')
del temp, dfSR_messageList
#phoneBillList
dfSR_phoneBillList=dfSocialRelations[['reportId','phoneBillList']]
dfSR_phoneBillList=getDFfromLists(dfSR_phoneBillList.copy(),True)
temp=pd.DataFrame(dfSR_phoneBillList['phoneBillList'].tolist(),index=dfSR_phoneBillList.index)
temp.insert(0,'reportId',dfSR_phoneBillList['reportId'])
insertTableIntoMyDB(temp,'socialRelationship_phoneBillList',schema='quantgroup_my')
del temp, dfSR_phoneBillList
#phoneInfoList
dfSR_phoneInfoList=dfSocialRelations[['reportId','phoneInfoList']]
dfSR_phoneInfoList=getDFfromLists(dfSR_phoneInfoList.copy(),True)
temp=pd.DataFrame(dfSR_phoneInfoList['phoneInfoList'].tolist(),index=dfSR_phoneInfoList.index)
temp.insert(0,'reportId',dfSR_phoneInfoList['reportId'])
insertTableIntoMyDB(temp,'socialRelationship_phoneInfoList',schema='quantgroup_my')
del temp, dfSR_phoneInfoList
#phoneListAll
dfSR_phoneListAll=dfSocialRelations[['reportId','phoneListAll']]
dfSR_phoneListAll=getDFfromLists(dfSR_phoneListAll.copy(),True)
temp=pd.DataFrame(dfSR_phoneListAll['phoneListAll'].tolist(),index=dfSR_phoneListAll.index)
temp.insert(0,'reportId',dfSR_phoneListAll['reportId'])
insertTableIntoMyDB(temp,'socialRelationship_phoneListAll',schema='quantgroup_my')
del temp, dfSR_phoneListAll
#phoneWithCompanyList
dfSR_phoneWithCompanyList=dfSocialRelations[['reportId','phoneWithCompanyList']]
dfSR_phoneWithCompanyList=getDFfromLists(dfSR_phoneWithCompanyList.copy(),True)
temp=pd.DataFrame(dfSR_phoneWithCompanyList['phoneWithCompanyList'].tolist(),index=dfSR_phoneWithCompanyList.index)
temp.insert(0,'reportId',dfSR_phoneWithCompanyList['reportId'])
insertTableIntoMyDB(temp,'socialRelationship_phoneWithCompanyList',schema='quantgroup_my')
del temp, dfSR_phoneWithCompanyList
del dfSocialRelations
dfQG_json.drop('socialRelationship',axis=1,inplace=True)

"""policy"""
dfPolicy=dfQG_json[['id','policy']]
dfPolicy=dfPolicy.loc[(dfPolicy['policy'].notnull()) & (dfPolicy['policy']!=dict())]
#*****************
temp=pd.DataFrame(dfPolicy['policy'].tolist(),index=dfPolicy.index)
temp.insert(0,'reportId',dfPolicy['id'])
dfPolicy=temp
del temp
#policyInfoList
dfPolicy_policyInfoList=dfPolicy[['reportId','policyInfoList']]
dfPolicy_policyInfoList=getDFfromLists(dfPolicy_policyInfoList.copy(),True)
temp=pd.DataFrame(dfPolicy_policyInfoList['policyInfoList'].tolist(),index=dfPolicy_policyInfoList.index)
temp.insert(0,'reportId',dfPolicy_policyInfoList['reportId'])
insertTableIntoMyDB(temp,'policy_policyInfoList',schema='quantgroup_my')
del temp, dfPolicy_policyInfoList
#main
dfPolicy.drop('policyInfoList',axis=1,inplace=True)
insertTableIntoMyDB(dfPolicy,'policy_main',schema='quantgroup_my')
del dfPolicy
dfQG_json.drop('policy',axis=1,inplace=True)

"""behavior info"""
dfBehavior=dfQG_json[['id','behaviorInfo']]
dfBehavior=dfBehavior.loc[(dfBehavior['behaviorInfo'].notnull()) & (dfBehavior['behaviorInfo']!=dict())]
#*****************
temp=pd.DataFrame(dfBehavior['behaviorInfo'].tolist(),index=dfBehavior.index)
temp.insert(0,'reportId',dfBehavior['id'])
dfBehavior=temp
del temp
#flowFeatureList
dfBehavior_flowFeatureList=dfBehavior[['reportId','flowFeatureList']]
dfBehavior_flowFeatureList=getDFfromLists(dfBehavior_flowFeatureList.copy(),True)
temp=pd.DataFrame(dfBehavior_flowFeatureList['flowFeatureList'].tolist(),index=dfBehavior_flowFeatureList.index)
temp.insert(0,'reportId',dfBehavior_flowFeatureList['reportId'])
insertTableIntoMyDB(temp,'behaviorInfo_flowFeatureList',schema='quantgroup_my')
del temp, dfBehavior_flowFeatureList
del dfBehavior
dfQG_json.drop('behaviorInfo',axis=1,inplace=True)

"""credit info"""
dfCredit=dfQG_json[['id','creditInfo']]
dfCredit=dfCredit.loc[(dfCredit['creditInfo'].notnull()) & (dfCredit['creditInfo']!=dict())]
#*****************
temp=pd.DataFrame(dfCredit['creditInfo'].tolist(),index=dfCredit.index)
temp.insert(0,'reportId',dfCredit['id'])
dfCredit=temp
del temp
#creditCardBillList
dfCredit_creditCardBillList=dfCredit[['reportId','creditCardBillList']]
dfCredit_creditCardBillList=dfCredit_creditCardBillList.loc[(dfCredit_creditCardBillList['creditCardBillList'].notnull()) & (dfCredit_creditCardBillList['creditCardBillList']!=dict())]
temp=pd.DataFrame(dfCredit_creditCardBillList['creditCardBillList'].tolist(),index=dfCredit_creditCardBillList.index)
temp.insert(0,'reportId',dfCredit_creditCardBillList['reportId'])
insertTableIntoMyDB(temp,'creditInfo_creditCardBillList',schema='quantgroup_my')
del temp, dfCredit_creditCardBillList
#userCredit
dfCredit_userCredit=dfCredit[['reportId','userCredit']]
dfCredit_userCredit=dfCredit_userCredit.loc[(dfCredit_userCredit['userCredit'].notnull()) & (dfCredit_userCredit['userCredit']!=dict())]
temp=pd.DataFrame(dfCredit_userCredit['userCredit'].tolist(),index=dfCredit_userCredit.index)
temp.insert(0,'reportId',dfCredit_userCredit['reportId'])
insertTableIntoMyDB(temp,'creditInfo_userCredit',schema='quantgroup_my')
del temp, dfCredit_userCredit
#creditInvestigationOther
dfCredit_creditInvestigationOther=dfCredit[['reportId','creditInvestigationOther']]
dfCredit_creditInvestigationOther=getDFfromLists(dfCredit_creditInvestigationOther.copy(),True)
temp=pd.DataFrame(dfCredit_creditInvestigationOther['creditInvestigationOther'].tolist(),index=dfCredit_creditInvestigationOther.index)
temp.insert(0,'reportId',dfCredit_creditInvestigationOther['reportId'])
insertTableIntoMyDB(temp,'creditInfo_creditInvestigationOther',schema='quantgroup_my')
del temp, dfCredit_creditInvestigationOther
del dfCredit
dfQG_json.drop('creditInfo',axis=1,inplace=True)

"""financial info"""
dfFinancial=dfQG_json[['id','financialInfo']]
dfFinancial=dfFinancial.loc[(dfFinancial['financialInfo'].notnull()) & (dfFinancial['financialInfo']!=dict())]
#*****************
temp=pd.DataFrame(dfFinancial['financialInfo'].tolist(),index=dfFinancial.index)
temp.insert(0,'reportId',dfFinancial['id'])
dfFinancial=temp
del temp
#incomeInfo
dfFinancial_incomeInfo=dfFinancial[['reportId','incomeInfo']]
dfFinancial_incomeInfo=dfFinancial_incomeInfo.loc[(dfFinancial_incomeInfo['incomeInfo'].notnull()) & (dfFinancial_incomeInfo['incomeInfo']!=dict())]
temp=pd.DataFrame(dfFinancial_incomeInfo['incomeInfo'].tolist(),index=dfFinancial_incomeInfo.index)
temp.insert(0,'reportId',dfFinancial_incomeInfo['reportId'])
insertTableIntoMyDB(temp,'financialInfo_incomeInfo',schema='quantgroup_my')
del temp, dfFinancial_incomeInfo
del dfFinancial
dfQG_json.drop('financialInfo',axis=1,inplace=True)

"""personal info"""
dfPersonal=dfQG_json[['id','personalInfo']]
dfPersonal=dfPersonal.loc[(dfPersonal['personalInfo'].notnull()) & (dfPersonal['personalInfo']!=dict())]
#*****************
temp=pd.DataFrame(dfPersonal['personalInfo'].tolist(),index=dfPersonal.index)
temp.insert(0,'reportId',dfPersonal['id'])
dfPersonal=temp
del temp
#baseInfo
dfPersonal_baseInfo=dfPersonal[['reportId','baseInfo']]
dfPersonal_baseInfo=dfPersonal_baseInfo.loc[(dfPersonal_baseInfo['baseInfo'].notnull()) & (dfPersonal_baseInfo['baseInfo']!=dict())]
temp=pd.DataFrame(dfPersonal_baseInfo['baseInfo'].tolist(),index=dfPersonal_baseInfo.index)
temp.insert(0,'reportId',dfPersonal_baseInfo['reportId'])
insertTableIntoMyDB(temp,'personalInfo_baseInfo',schema='quantgroup_my')
del temp, dfPersonal_baseInfo
#matchInfoList
dfPersonal_matchInfoList=dfPersonal[['reportId','matchInfoList']]
dfPersonal_matchInfoList=getDFfromLists(dfPersonal_matchInfoList.copy(),True)
temp=pd.DataFrame(dfPersonal_matchInfoList['matchInfoList'].tolist(),index=dfPersonal_matchInfoList.index)
temp.insert(0,'reportId',dfPersonal_matchInfoList['reportId'])
insertTableIntoMyDB(temp,'personalInfo_matchInfoList',schema='quantgroup_my')
del temp, dfPersonal_matchInfoList
del dfPersonal
dfQG_json.drop('personalInfo',axis=1,inplace=True)

"""trade info"""
dfTrade=dfQG_json[['id','tradeInfo']]
dfTrade=dfTrade.loc[(dfTrade['tradeInfo'].notnull()) & (dfTrade['tradeInfo']!=dict())]
#*****************
temp=pd.DataFrame(dfTrade['tradeInfo'].tolist(),index=dfTrade.index)
temp.insert(0,'reportId',dfTrade['id'])
dfTrade=temp
del temp
#alipayInfo
dfTrade_alipayInfo=dfTrade[['reportId','alipayInfo']]
dfTrade_alipayInfo=dfTrade_alipayInfo.loc[(dfTrade_alipayInfo['alipayInfo'].notnull()) & (dfTrade_alipayInfo['alipayInfo']!=dict())]
temp=pd.DataFrame(dfTrade_alipayInfo['alipayInfo'].tolist(),index=dfTrade_alipayInfo.index)
temp.insert(0,'reportId',dfTrade_alipayInfo['reportId'])
insertTableIntoMyDB(temp,'tradeInfo_alipayInfo',schema='quantgroup_my')
del temp, dfTrade_alipayInfo
#tradeAnalyze
dfTrade_tradeAnalyze=dfTrade[['reportId','tradeAnalyze']]
dfTrade_tradeAnalyze=dfTrade_tradeAnalyze.loc[(dfTrade_tradeAnalyze['tradeAnalyze'].notnull()) & (dfTrade_tradeAnalyze['tradeAnalyze']!=dict())]
temp=pd.DataFrame(dfTrade_tradeAnalyze['tradeAnalyze'].tolist(),index=dfTrade_tradeAnalyze.index)
temp.insert(0,'reportId',dfTrade_tradeAnalyze['reportId'])
insertTableIntoMyDB(temp,'tradeInfo_tradeAnalyze',schema='quantgroup_my')
del temp, dfTrade_tradeAnalyze
#orderMap
dfTrade_orderMap=dfTrade[['reportId','orderMap']]
dfTrade_orderMap=getDFfromLists(dfTrade_orderMap.copy(),True)
temp=pd.DataFrame(dfTrade_orderMap['orderMap'].tolist(),index=dfTrade_orderMap.index)
temp.insert(0,'reportId',dfTrade_orderMap['reportId'])
insertTableIntoMyDB(temp,'tradeInfo_orderMap',schema='quantgroup_my')
del temp, dfTrade_orderMap
del dfTrade
dfQG_json.drop('tradeInfo',axis=1,inplace=True)
"""main info JSON"""
dfQG_json.rename(columns={"id": "reportId"},inplace=True)
insertTableIntoMyDB(dfQG_json,'json_main',schema='quantgroup_my')
del dfQG_json

"""last_login_request"""
df_llr=getTableFromMyDB('quantgroup_all',req='id, last_login_request',showTime=True)
df_llr=df_llr.loc[(df_llr['last_login_request'].notnull()) & (df_llr['last_login_request']!=dict())]
temp=pd.DataFrame(df_llr['last_login_request'].tolist(),index=df_llr.index)
temp.drop('id',axis=1,inplace=True)
temp.insert(0,'reportId',df_llr['id'])
insertTableIntoMyDB(temp,'last_login_request',schema='quantgroup_my')
del temp, df_llr

"""error"""
df_error=getTableFromMyDB('quantgroup_all',req='id, error',showTime=True)
df_error=getDFfromLists(df_error.copy(),True)
df_error.rename(columns={"id": "reportId"},inplace=True)
insertTableIntoMyDB(df_error,'error',schema='quantgroup_my')
del df_error

"""main"""
columns=list(infoQuant.loc[(infoQuant['depth']==1) & (infoQuant['types']!="<class 'list'>") & (infoQuant['types']!="<class 'dict'>"),'colName'])
dfMain=df_llr=getTableFromMyDB('quantgroup_all',req=','.join(columns),showTime=True)
dfMain.rename(columns={"id": "reportId","application_id":"id"},inplace=True)
insertTableIntoMyDB(dfMain,'main',schema='quantgroup_my')
del dfMain, columns




"""MY MINSHI"""
columns=infoAppl.loc[(infoAppl['partNotNA']>0) & (infoAppl['depth']==1) & ((infoAppl['types']=="<class 'list'>") | (infoAppl['types']=="<class 'dict'>")),['colName','types']]