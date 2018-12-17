# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 18:26:51 2017

@author: Alex
"""
def notnullByWeek_oneVar(dfInput, variable, showTime = True):
    timeHelp = time.time()
    dfOutput = pd.DataFrame({'YW': 0, 'YD': dfInput['YD'].unique(), variable: 0})
    dfOutput = dfOutput.sort_values('YD')
    dfOutput.index = range(dfOutput.shape[0])
    for i in range(dfOutput.shape[0]):
        dfOutput.loc[i, variable] = round(100 * sum(dfInput.loc[dfInput['YD'] == dfOutput.loc[i, 'YD'],variable].notnull()) / sum(dfInput['YD'] == dfOutput.loc[i, 'YD']), 2)
        dfOutput.loc[i, 'YW'] = dfInput.loc[dfInput['YD'] == dfOutput.loc[i, 'YD'], 'YW'].unique()
    if showTime:
        print('notNullByWeek: ', round(time.time() - timeHelp, 2), ' sec')
    return dfOutput

def notnullByWeek_someVars(query_vars, tableName = 'applications', db = 'scorVietnam',
                           schema = os.environ['mf_db'], where = '', showTime = True,  #possible production
                           iovations = False
    ): 
    timeHelp = time.time()
    print()
    if not iovations:
        df = getTableFromMyDB(tableName, db, schema, req = query_vars, where = where)
    else:
        q = 'SELECT ' + query_vars + ', device_alias as iovation_device_alias FROM '
        q = q + schema + '.applications LEFT JOIN ' + schema
        q = q + '.application_iovations ON applications.id=application_id'
        q = q + " WHERE status='accepted' AND type IN ('Application::Sale','Application::Web')"
        df = getQueryFromMyDB(db, q)
        del q
    variables = list(df.columns)
    variables.remove('uid')
    variables.remove('created_at')
    df['YW'] = list(map(lambda x:x.year * 100 + x.weekofyear, df['created_at']))
    df['YD'] = list(map(lambda x:x.year * 1000 + x.dayofyear, df['created_at']))
    del df['created_at']
    print('_______________')
    flag = 0
    for variable in variables:
        print(variable)
        df_step = notnullByWeek_oneVar(df[['YW', 'YD', variable]], variable)
        if flag == 0:
            dfOutput = df_step.copy()
            flag = 1
        else:
            dfOutput[variable] = df_step[variable]
        print('_______________')
    dfOutput['cnt'] = 0
    for YD in dfOutput['YD']:
        dfOutput.loc[dfOutput['YD'] == YD, 'cnt'] = sum(df['YD'] == YD)
    if showTime:
        print('notNullByWeek_someVars: ', round((time.time() - timeHelp) / 60, 2), ' min.')
    return dfOutput    

def getVariables(titles, replacement, columns):
    variables = titles.copy()
    if 'iovation_device_alias' in variables:
        variables.remove('iovation_device_alias')
    for i in range(len(variables)):
        if variables[i] not in columns:
            flag=0
            for var in replacement.loc[replacement['title'] == variables[i], 'variation']:
                if var in columns:
                    variables[i] = var + ' as ' + titles[i]
                    flag = 1
                    break
            if flag == 0:
                variables[i] = ''
    variables = list(filter(lambda x: x != '', variables))
    if 'uid' in columns:
        return 'uid, created_at, ' + ', '.join(variables), variables
    else:
        return 'gate_id as uid, created_at, ' + ', '.join(variables), variables
            
    
def getDataInfo(country = 'Vietnam', schema = '', where = ''):
    df = pd.DataFrame({'country': country, 'schema': schema}, index = [0])
    df['all'] = getTableFromMyDB(db = 'scor' + country, schema = schema,
      req = 'count(*)', where = '', showTime = False)
    df['accepted'] = getTableFromMyDB(db = 'scor' + country, schema = schema,
      req = 'count(*)', where = "status='accepted'", showTime = False)
    df['rejected'] = getTableFromMyDB(db = 'scor' + country, schema = schema,
      req = 'count(*)', where = "status='rejected'", showTime = False)
    df['Web'] = getTableFromMyDB(db = 'scor' + country, schema = schema,
      req = 'count(*)', where = "type='Application::Web'", showTime = False)
    df['Sale'] = getTableFromMyDB(db = 'scor' + country, schema = schema,
      req = 'count(*)', where = "type='Application::Sale'", showTime = False)
    df['CreditLine'] = getTableFromMyDB(db = 'scor' + country, schema = schema,
      req = 'count(*)', where = "type='Application::CreditLine'", showTime = False)
    if 'uid' in list((getTableFromMyDB('applications', db = 'scor' + country,
                schema = schema, req = '*', limit = ' limit 1', showTime = False)).columns):
        q_target = 'SELECT count(applications.uid) FROM ' + schema
        q_target = q_target + '.applications INNER JOIN public.target ON lower(applications.uid)=lower(target.uid)'
    else:
        q_target = 'SELECT count(applications.gate_id) FROM ' + schema
        q_target = q_target + '.applications INNER JOIN public.target ON lower(applications.gate_id)=lower(target.uid)'
    where_deviseIn = 'iovation_device_alias IS NOT NULL'
    q_deviseOut = 'SELECT count(applications.id) FROM ' + schema + '.applications INNER JOIN '
    q_deviseOut = q_deviseOut +schema+'.application_iovations ON applications.id=application_iovations.application_id WHERE device_alias IS NOT NULL'
    if where != '':
        q_target = q_target + ' WHERE ' + where
        where_deviseIn = where_deviseIn + ' AND ' + where
        q_deviseOut = q_deviseOut + ' AND ' + where
    df['target'] = getQueryFromMyDB(db = 'scor' + country, query = q_target, showTime = False)
    df['target_all'] = getTableFromMyDB(tableName = 'target',db = 'scor' + country,
      schema = 'public', req = 'count(*)', showTime = False)
    df['deviceIn'] = getTableFromMyDB(db = 'scor' + country, schema = schema, 
      req = 'count(*)', where = where_deviseIn, showTime = False)
    df['deviceOut'] = getQueryFromMyDB(db = 'scor' + country, query = q_deviseOut, showTime = False)
    return df

def getDataInfo_some(info):
    info.index = range(info.shape[0])
    flag = 0
    for i in range(info.shape[0]):
        df_step = getDataInfo(info.loc[i,'country'], info.loc[i,'schema'], info.loc[i,'where'])
        if flag == 0:
            flag = 1
            df = df_step
        else:
            df = df.append(df_step, ignore_index = True)
    return df
