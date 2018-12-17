# -*- coding: utf-8 -*-
"""
Created on Wed May 17 16:00:44 2017

@author: aleks
"""

"""UNPACKING JSON"""
def getTypesOfColumn(df,depth=1,knot='',mainKnot='',showTime=False):
    time_help=time.time()
    infoTypes=pd.DataFrame({'colName':df.columns,'types':'','amUnique':-5,'partNotNA':0,'depth':depth,'knot':knot,'mainKnot':mainKnot,'typeInList':''})
    for column in df.columns:
        cond= df[column].notnull()

        temp=list(set(list(map(type,df.loc[cond,column]))))        
        if type(None) in temp:
            temp.remove(type(None))
            print(column,':there is None')
        if pd.tslib.NaTType in temp:
            temp.remove(pd.tslib.NaTType)
            print(column,':there is NaTType')

        if len(temp)==1 :
            infoTypes.loc[infoTypes['colName']==column,'types']=temp
            if temp[0]==list:
                cond=(cond) & (pd.Series(list(map(lambda x: x!=[],df[column])),index=df.index))
                if sum(cond)==0:
                    infoTypes.loc[infoTypes['colName']==column,'typeInList']=0
                else:
                    try:        
                        infoTypes.loc[infoTypes['colName']==column,'typeInList']=max(set(list(map(typeOfList,df.loc[cond,column]))))             
                    except:
                            try:
                                infoTypes.loc[infoTypes['colName']==column,'typeInList']=set(list(map(typeOfList,df.loc[cond,column])))
                            except:
                                print('PROBLEM WITH: '+column,', type:',temp,'(cant get type in list)')
            if temp[0]==dict:
                cond=(cond) & (pd.Series(list(map(lambda x: x!=dict(),df[column])),index=df.index))
            infoTypes.loc[infoTypes['colName']==column,'partNotNA']=round(100*sum(cond)/len(df[column]),2)    
            #**************************
            try:        
                infoTypes.loc[infoTypes['colName']==column,'amUnique']=getAmountOfUnique(df.loc[cond,column],temp[0])
            except:
                print('PROBLEM WITH: '+column,', type:',temp,'(cant count amount of unique values)')
            #-1: list & dict; -2:dict; -3:list
            #**************************
        elif len(temp)>1:
            print ('getTypesOf: ',column,':',temp)
    if mainKnot=='':
        cond=(infoTypes['types']==dict) | (infoTypes['amUnique']==-2)
        infoTypes.loc[cond,'mainKnot']=infoTypes.loc[cond,'colName']
    if showTime:
        print(round(time.time()-time_help,2))
    return infoTypes

def getAmountOfUnique(ds,typeDs):
    #cond не обязательно. тк мы изначально загружаем проверенный ds
    cond= ds.notnull()
    if typeDs==dict:
        cond=(cond) & (pd.Series(list(map(lambda x: x!=dict(),ds)),index=ds.index)) 
        return len(pd.DataFrame(ds.loc[cond].tolist()).columns)
    elif typeDs==list:
        cond=(cond) & (pd.Series(list(map(lambda x: x!=[],ds)),index=ds.index)) 
        listType=set(list(map(typeOfList,ds.loc[cond])))
        if (('dict' in listType) | ('dict+' in listType)) & (('list' in listType) | ('list+' in listType)):
            return -1
        elif ('dict' in listType) | ('dict+' in listType):
            return -2
        elif ('list' in listType) | ('list+' in listType):
            return -3
        else:
            valueList=[]
            for value in ds.loc[cond]:
                valueList.extend(list(value))
                valueList=list(set(valueList))
            return len(valueList)
    else:
        return len(set(ds.loc[cond].values))
    
def typeOfList(myList):
    if (type(myList)!=list) | (len(myList)==0):
        print ('type of list: WTF???')
        return 'WTF'
    types=set(list(map(type,myList)))
    if dict in types:
        if len(types)>1:
            return 'dict+' 
        return 'dict'
    elif list in types:
        if len(types)>1:
            return 'list+'
        return 'list'
    elif (set([int]) == types) | (set([np.int64]) == types) | (set([np.int64,int]) == types):
        return 'int'
    elif (set([float]) == types) | (set([np.float64]) == types) | (set([np.float64,float]) == types):
        return 'float'            
    elif (len(types)==1) & (str in types):
        return max(list(map(len,myList)))
    elif len(types)==1:
        return list(types)[0]
    else:
        return 'scalar+'

def getLeaf(leafName,infoDf,df,depth,mainKnot,parse=True):
    time_help=time.time()
    leafGraph=pd.DataFrame({'leaf':leafName,'depth':depth,'mainKnot':mainKnot},index=[0])
    if depth>1:
        for i in range(depth-1,0,-1):
            cond=(infoDf['colName']==leafName) & (infoDf['mainKnot']==mainKnot) & (infoDf['depth']==i+1) & ((infoDf['types']==dict) | (infoDf['amUnique']==-2))
            leafName=infoDf.loc[cond,'knot'].values[0]  
            if sum(cond)>1:
                print('Main knot:',mainKnot,', leaf name:',leafName,', depth:',i,', options:',infoDf.loc[cond,'knot'].values)
            elif sum(cond)==0:
                print('Main knot:',mainKnot,', leaf name:',leafName,', depth:',i,': no options')
            leafGraph=leafGraph.append(pd.DataFrame({'leaf':leafName,'depth':i,'mainKnot':mainKnot},index=[0]),ignore_index=True)

    if leafGraph.loc[leafGraph.depth==1,'leaf'].values[0]!=mainKnot:
        print ('getLeaf: DIFFERENT MAIN KNOTS')    
    dfLeaf=df[mainKnot]
    currentKnot=mainKnot
    print('prework:',round(time.time()-time_help,2),' sec.')
    if leafGraph.depth.max()>1:
        for i in range(2,leafGraph['depth'].max()+1):
            time_help=time.time()
            if sum((infoDf['amUnique']==-2) & (infoDf['colName']==currentKnot))==1:
                cond=(dfLeaf.notnull()) & (pd.Series(list(map(lambda x: (x!=[]) & (x!=[dict()]),dfLeaf)),index=dfLeaf.index))
                dfLeaf=getDSfromLists(dfLeaf.loc[cond])
                print(i-1,'.',currentKnot,'-',mainKnot,':',round(time.time()-time_help,2),' sec., List to dict.')
                time_help=time.time()
            cond=(dfLeaf.notnull()) & (pd.Series(list(map(lambda x: x!=dict(),dfLeaf)),index=dfLeaf.index))
            dfLeaf=pd.DataFrame(dfLeaf.loc[cond].tolist())
            print(i-1,'.',currentKnot,'-',mainKnot,':',round(time.time()-time_help,2),' sec.')
            currentKnot=leafGraph.loc[leafGraph['depth']==i,'leaf'].values[0]
            dfLeaf=dfLeaf[currentKnot]
            
    if parse:
        time_help=time.time()
        if sum((infoDf.amUnique==-2) & (infoDf.colName==currentKnot))==1:
            cond=(dfLeaf.notnull()) & (pd.Series(list(map(lambda x: (x!=[]) & (x!=[dict()]),dfLeaf)),index=dfLeaf.index))
            dfLeaf=getDSfromLists(dfLeaf.loc[cond])
            print(leafGraph['depth'].max(),'.',currentKnot,'-',mainKnot,':',round(time.time()-time_help,2),' sec., List to dict.')
            time_help=time.time()
        cond=(dfLeaf.notnull()) & (pd.Series(list(map(lambda x: x!=dict(),dfLeaf)),index=dfLeaf.index))
        dfLeaf=pd.DataFrame(dfLeaf.loc[cond].tolist())
        print(leafGraph['depth'].max(),'.',currentKnot,'-',mainKnot,':',round(time.time()-time_help,2),' sec.')
    return dfLeaf

def getDSfromLists(ds):
    flag=0
    for element in ds[ds.notnull()]:
        if flag==0:
            flag=1
            dsOut=pd.Series(element)    
        else:
            dsOut=dsOut.append(pd.Series(element),ignore_index=True)
    return dsOut

def getTree(df,showLogs=False,showTime=False):
    if showLogs:
        print('Program is working:',time.strftime("%H:%M:%S"))
        print('----------------------------')
    time_help=time.time()
    info=getTypesOfColumn(df)
    leafsCurrent=info.loc[((info.types==dict) | (info.amUnique==-2)) & (info.partNotNA>0),['colName','depth','types','mainKnot']]
    leafsCurrent.index=range(leafsCurrent.shape[0])
    if showLogs:
        print('****************************************************************')
        print()
        print(time.strftime("%H:%M:%S"))
        print('Amount of variables:',len(info['colName']))
        print('------------------------')
        print ('Now we will work on this variables:')
        print (leafsCurrent[['colName','depth','types']])
        print('---------------------------------------------')
        
    while(leafsCurrent.shape[0]>0):
        tempLeaf=leafsCurrent.loc[leafsCurrent.depth==leafsCurrent.depth.max()]
        currentLeaf=tempLeaf.colName[min(tempLeaf.index)]
        currentDepth=tempLeaf.depth[min(tempLeaf.index)]
        currentMainKnot=tempLeaf.mainKnot[min(tempLeaf.index)]
        del tempLeaf
        if showLogs:
            print('Current leaf: ',currentLeaf,', depth:',currentDepth)
            print('---------------------------------------------')            
            print()
        dfLeaf=getLeaf(currentLeaf,info,df,currentDepth,currentMainKnot)
        infoStep=getTypesOfColumn(dfLeaf,depth=currentDepth+1,knot=currentLeaf,mainKnot=currentMainKnot)
        """сливаем info и info_step"""
        info=info.append(infoStep, ignore_index=True)
        """Удаляем из leafs_current строку с leaf"""
        cond=(leafsCurrent['colName']==currentLeaf) & (leafsCurrent['depth']==currentDepth) & (leafsCurrent['mainKnot']==currentMainKnot)
        leafsCurrent=leafsCurrent.loc[list(map(lambda x: not x,cond))]      
        """Добавляем в leafs_current строки с новыми json"""
        cond=((infoStep.types==dict) | (infoStep.amUnique==-2)) & (infoStep.partNotNA>0)
        if sum(cond)>0:
            leafsCurrent=leafsCurrent.append(infoStep.loc[cond,['colName','depth','types','mainKnot']], ignore_index=True)
        if (showLogs) & (leafsCurrent.shape[0]>0):
            leafsCurrent.index=range(leafsCurrent.shape[0])
            print('****************************************************************')
            print()
            print(time.strftime("%H:%M:%S"))
            print('Amount of variables:',len(info['colName']))
            print('------------------------')
            print ('Now we will work on this variables:')
            print (leafsCurrent[['colName','depth','types']])
            print('---------------------------------------------') 
    if showTime:
        print(round(time.time()-time_help,2))
    return info


"""Разбиваем на подтаблицы"""
def getDFfromLists(df,isId=False,showTime=True):#id & ds
    time_help=time.time()    
    flag=0
    nameId=df.columns[0]
    nameCol=df.columns[1]
    df=df.loc[(df[nameCol].notnull()) & (pd.Series(list(map(lambda x:x!=[],df[nameCol])),index=df.index))]
    dfOut=pd.DataFrame()
    dfFinal=pd.DataFrame()
    
    for Id in df[nameId]:
        List=list(df.loc[df[nameId]==Id,nameCol])[0]
        dfStep=pd.DataFrame({nameId:[Id]*len(List),nameCol:List})
        dfOut=dfOut.append(dfStep,ignore_index=True)
        if dfOut.shape[0]>50000:
            dfFinal=dfFinal.append(dfOut,ignore_index=True)
            dfOut=pd.DataFrame()
    #****************************
    if dfOut.shape[0]>0:
        dfFinal=dfFinal.append(dfOut,ignore_index=True)
    if showTime:
        print(round(time.time()-time_help,2),' sec.')
    if isId:
        return dfFinal
    else:
        return dfFinal[nameCol]




def getLeafUpdate(leafName,infoTable,df,parse=True,colIdName='id'):
    depth=infoTable.loc[infoTable.colName==leafName,'depth'].values[0]
    leafGraph=pd.DataFrame({'leaf':leafName,'depth':depth},index=[0])
    if depth>1:
        for i in range(depth-1,0,-1):
          leafName=infoTable.loc[infoTable.colName==leafName,'knot'].values[0]  
          depth=infoTable.loc[infoTable.colName==leafName,'depth'].values[0]
          leafGraph=leafGraph.append(pd.DataFrame({'leaf':leafName,'depth':depth},index=[0]),ignore_index=True)
    leafName=leafGraph.loc[leafGraph.depth==1,'leaf'].values[0]
    dfLeaf=df.loc[df[leafName].notnull(),[colIdName,leafName]]
    
    if leafGraph.depth.max()>1:
        for i in range(2,leafGraph.depth.max()+1):
            if sum((infoTable.amUnique==-2) & (infoTable.colName==leafName))==1:
                dfLeaf=getParseLists(dfLeaf,leafName)
            #**********************
            cond=dfLeaf[leafName].notnull()
            colId=dfLeaf.loc[cond,colIdName]
            dfLeaf=pd.DataFrame(dfLeaf.loc[cond,leafName].tolist())
            dfLeaf.insert(0,colIdName,colId)
            dfLeaf.index==range(dfLeaf.shape[0])
            #**********************            
            leafName=leafGraph.loc[leafGraph.depth==i,'leaf'].values[0]
            dfLeaf=dfLeaf[[colIdName,leafName]]
    if parse:
        if sum((infoTable.amUnique==-2) & (infoTable.colName==leafName))==1:
            dfLeaf=getParseLists(dfLeaf,leafName)
        cond=dfLeaf[leafName].notnull()
        colId=dfLeaf.loc[cond,colIdName]
        dfLeaf=pd.DataFrame(dfLeaf.loc[cond,leafName].tolist())
        dfLeaf.insert(0,colIdName,colId)
        dfLeaf.index==range(dfLeaf.shape[0])
    return dfLeaf

def getParseLists(df,leafName,colIdName='id'):
    flag=0
    #На notnull смысла проверять нет 
    for element in df.loc[df[leafName]!=[],leafName]:
        colId=df.loc[df[leafName]==element,colIdName].values[0]
        dfStep=pd.DataFrame({colIdName:colId,leafName:element})
        if flag==0:
            flag=1
            dfOut=dfStep    
        else:
            dfOut=dfOut.append(dfStep,ignore_index=True)
    return dsOut