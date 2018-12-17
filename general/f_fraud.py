# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 23:56:43 2017

@author: Alex
"""
#FRAUD1
def getHour2017(created_at):
    delta_days=(created_at.date() - datetime.date(2016, 1, 1)).days
    delta_hours = created_at.time().hour + created_at.time().minute / 60 + created_at.time().second / 3600
    return 24 * delta_days + delta_hours

def getDelta_Fraud(dfIssue, dfApplication, variableMain, variableDiff = ''):
    """
    For each uid we get a list of HOURS when there was
    application with the same variable, keep on only that occcur
    no earlier than 1 hour before the current application
    ***********************
    FRAUD RULE 1(mass application)
    df includes: uid, hours2017,'variable'
        hours2017 - amount of hours since 01.01.2017
    """
    #******************************************
    """
    We reduce the tables, keep on only those rows that:
        1. occur more than 1 time with one 'variable' in dfApplication
        2. occur at least once in dfIssue (по 'variable')
    """
    varFraud = list(dfApplication[variableMain].value_counts().loc[dfApplication[variableMain].value_counts() > 1].index)
    dfIssueSmall = dfIssue.loc[list(map(lambda x: x in set(varFraud), dfIssue[variableMain]))]
    varFraud = list(dfIssueSmall[variableMain].unique())
    dfApplicationSmall = dfApplication.loc[list(map(lambda x:x in set(varFraud), dfApplication[variableMain]))]
    if variableDiff == '':
        dfIssueSmall.columns = ['uid', 'hours2017', 'variableMain']
        dfApplicationSmall.columns = ['a_uid', 'a_hours2017', 'a_variableMain']
        #******************************************
        query = '''
        SELECT uid, hours2017-a_hours2017 AS delta_hours
        FROM dfIssueSmall, dfApplicationSmall
        WHERE uid<>a_uid AND variableMain=a_variableMain AND delta_hours>=-1
        '''
    else:
        dfIssueSmall.columns = ['uid', 'hours2017','variableMain','variableDiff']
        dfApplicationSmall.columns = ['a_uid','a_hours2017', 'a_variableMain','a_variableDiff']
        #******************************************
        query = """
        SELECT uid, hours2017-a_hours2017 AS delta_hours
        FROM dfIssueSmall, dfApplicationSmall
        WHERE uid<>a_uid AND variableMain=a_variableMain AND variableDiff<>a_variableDiff AND delta_hours>=-1
        """
    df = ps.sqldf(query, locals())
    return df


def getFlags_Fraud(df, hoursTo, cntRepeat):
    dfTo = df.loc[(df['delta_hours'] > 0) & (df['delta_hours'] < hoursTo), 'uid'].value_counts()
    dfTo = list(dfTo.loc[dfTo >= cntRepeat].index)
    dfWait = list(df.loc[df['delta_hours'] < 0, 'uid'])
    df = dfWait.copy()
    df.extend(dfTo)
    df = pd.DataFrame({'uid':list(set(df)), 'flagTo':0, 'flagWait':0})
    for i in range(df.shape[0]):
        df.loc[i,'flagTo'] = int(df.loc[i, 'uid'] in dfTo)
        df.loc[i, 'flagWait'] = int(df.loc[i, 'uid'] in dfWait) 
    return df[['uid', 'flagTo', 'flagWait']]

def getStatistic_Fraud(dfIssue, dfFlags, hoursTo, variableMain, variableDiff = '', cntRepeat = 1):
    dfIssue.columns = ['uid', 'target', 'variableMain']
    dfFlags.columns = ['uid', 'flagTo', 'flagWait']
    #***************
    if hoursTo % 24 == 0:
        timeTo = '(0)-(' + str(hoursTo // 24) + ')'
    else:
        timeTo = '(0)-(' + str(hoursTo//24) + ' d.' + str(hoursTo % 24) + ' h.)'
    #***************
    stat = pd.DataFrame({
            'variableMain':variableMain, 'variableDiff':variableDiff,
            'time':timeTo, 'hourTo':hoursTo}, index = [0]
    )
    stat['allCases'] = dfIssue.shape[0]
    stat['allBadCases'] = sum(dfIssue['target'] == 1)
    stat['nullCases'] = sum(dfIssue['variableMain'].isnull())
    stat['nullBadCases'] = sum((dfIssue['target'] == 1) & (dfIssue['variableMain'].isnull()))
    #***************
    query = """
     SELECT dfIssue.uid, variableMain, target, flagTo, flagWait
     FROM dfIssue LEFT JOIN dfFlags
     ON dfIssue.uid=dfFlags.uid
     """
    df = ps.sqldf(query, locals())
    df['flagTo'].fillna(0, inplace = True)
    df['flagWait'].fillna(0, inplace = True)
    #***************
    stat['fraudCases'] = sum(df['flagTo'])
    stat['fraudBadCases'] = sum((df['flagTo'] == 1) & (df['target'] == 1))
    stat['notFraudCases'] = sum(df['flagTo'] == 0)
    stat['notFraudBadCases'] = sum((df['flagTo'] == 0) & (df['target'] == 1))
    stat['futureInMainFraudCases'] = sum((df['flagTo'] == 1) & (df['flagWait'] == 1))
    stat['futureInMainFraudBadCases'] = sum((df['flagTo'] == 1) & (df['flagWait'] == 1) & (df['target'] == 1))
    stat['futureOutMainFraudCases'] = sum((df['flagTo'] == 0) & (df['flagWait'] == 1))
    stat['futureOutMainFraudBadCases'] = sum((df['flagTo'] == 0) & (df['flagWait'] == 1) & (df['target'] == 1))
    stat['clearCases'] = sum((df['flagTo'] == 0) & (df['flagWait'] == 0))
    stat['clearBadCases'] = sum((df['flagTo'] == 0) & (df['flagWait'] == 0) & (df['target'] == 1))
    return stat

def getInterval_Fraud(stat):
    stat.index = range(stat.shape[0])
    statInt = stat.copy()
    statInt.sort_values(by = 'hourTo', inplace=True)
    statInt.index = range(statInt.shape[0])
    hourFrom = statInt.loc[1:, 'hourTo']
    hourFrom[0] = 0
    statInt['hourFrom'] = hourFrom
    for i in range(statInt.shape[0]-1, 0, -1):
        statInt.loc[i,'time'] = statInt.loc[i, 'time'] + ' \ ' + statInt.loc[i-1, 'time']
    #***************
    for i in range(1, statInt.shape[0]):
        statInt.loc[i, 'fraudCases'] = stat.loc[i, 'fraudCases'] - stat.loc[i - 1, 'fraudCases']
        statInt.loc[i,'fraudBadCases'] = stat.loc[i, 'fraudBadCases'] - stat.loc[i - 1, 'fraudBadCases']
        statInt.loc[i, 'futureInMainFraudCases'] = stat.loc[i, 'futureInMainFraudCases'] - stat.loc[i - 1, 'futureInMainFraudCases']
        statInt.loc[i, 'futureInMainFraudBadCases'] = stat.loc[i, 'futureInMainFraudBadCases']-stat.loc[i - 1, 'futureInMainFraudBadCases']
        statInt.loc[i, 'futureOutMainFraudCases'] = stat.loc[i, 'futureOutMainFraudCases'] - stat.loc[i - 1, 'futureOutMainFraudCases']
        statInt.loc[i, 'futureOutMainFraudBadCases'] = stat.loc[i, 'futureOutMainFraudBadCases'] - stat.loc[i - 1, 'futureOutMainFraudBadCases']
    statInt['notFraudCases'] = statInt['allCases'] - statInt['fraudCases']
    statInt['notFraudBadCases'] = statInt['allBadCases'] - statInt['fraudBadCases']
    statInt['clearCases'] = statInt['allCases'] - statInt['fraudCases'] - statInt['futureOutMainFraudCases']
    statInt['clearBadCases'] = statInt['allBadCases'] - statInt['fraudBadCases'] - statInt['futureOutMainFraudBadCases']
    return statInt

def getRates(dfStat):
    #rates
    stat = dfStat.copy()
    stat['currentBR'] = round(100 * stat['allBadCases'] / stat['allCases'], 2)
    stat['nullBR'] = None
    stat.loc[stat['nullCases'] != 0, 'nullBR'] = round(100 * stat.loc[stat['nullCases'] != 0,'nullBadCases'] / stat.loc[stat['nullCases'] != 0, 'nullCases'], 2)        
    stat['fraudBR'] = None
    stat.loc[stat['fraudCases'] != 0, 'fraudBR'] = round(100 * stat.loc[stat['fraudCases'] != 0, 'fraudBadCases'] / stat.loc[stat['fraudCases'] != 0, 'fraudCases'], 2)
    stat['notFraudBR'] = None
    stat.loc[stat['notFraudCases'] != 0, 'notFraudBR'] = round(100 * stat.loc[stat['notFraudCases'] != 0, 'notFraudBadCases'] / stat.loc[stat['notFraudCases'] != 0, 'notFraudCases'], 2)
    stat['futureInMainBR'] = None
    stat.loc[stat['futureInMainFraudCases'] != 0,'futureInMainBR'] = round(100 * stat.loc[stat['futureInMainFraudCases'] != 0, 'futureInMainFraudBadCases'] / stat.loc[stat['futureInMainFraudCases'] != 0, 'futureInMainFraudCases'],2)
    stat['futureOutMainBR'] = None
    stat.loc[stat['futureOutMainFraudCases'] != 0, 'futureOutMainBR'] = round(100 * stat.loc[stat['futureOutMainFraudCases'] != 0, 'futureOutMainFraudBadCases'] / stat.loc[stat['futureOutMainFraudCases'] != 0, 'futureOutMainFraudCases'],2)
    stat.loc[(stat['futureOutMainFraudCases'] < 0) & (stat['futureOutMainBR'] > 0), 'futureOutMainBR'] = -stat.loc[(stat['futureOutMainFraudCases'] < 0) & (stat['futureOutMainBR'] > 0), 'futureOutMainBR']    
    stat['clearBR'] = None
    stat.loc[stat['clearCases'] != 0, 'clearBR'] = round(100 * stat.loc[stat['clearCases'] != 0, 'clearBadCases'] / stat.loc[stat['clearCases'] != 0, 'clearCases'], 2)
    #benefits
    stat['benefit_pp'] = None
    stat.loc[stat['notFraudBR'].notnull(), 'benefit_pp'] = stat.loc[stat['notFraudBR'].notnull(), 'currentBR'] - stat.loc[stat['notFraudBR'].notnull(), 'notFraudBR']
    stat['benefit_percent'] = None
    stat.loc[stat['notFraudBR'].notnull(), 'benefit_percent'] = round(-100 * (stat.loc[stat['notFraudBR'].notnull(), 'notFraudBR'] / stat.loc[stat['notFraudBR'].notnull(), 'currentBR'] - 1), 2)
    stat['benefitClear_pp'] = None
    stat.loc[stat['clearBR'].notnull(), 'benefitClear_pp'] = stat.loc[stat['clearBR'].notnull(), 'currentBR'] - stat.loc[stat['clearBR'].notnull(), 'clearBR']
    stat['benefitClear_percent'] = None
    stat.loc[stat['clearBR'].notnull(), 'benefitClear_percent'] = round(-100 * (stat.loc[stat['clearBR'].notnull(), 'clearBR'] / stat.loc[stat['clearBR'].notnull(), 'currentBR'] - 1), 2)
    #hitRates
    stat['hitRate'] = round(100 * stat['fraudCases'] / stat['allCases'], 2)
    stat['hitExtRate'] = round(100 * (stat['fraudCases'] + stat['futureOutMainFraudCases']) / stat['allCases'], 2)
    stat['nullRate'] = round(100 * stat['nullCases'] / stat['allCases'], 2)
    return stat
    

def Fraud1(dfIssue, dfApplication, variables, timeIntervals, cntRepeat = 1, showLogs = False):
    time_global = time.time()
    dfIssue.index = range(dfIssue.shape[0])
    dfIssue['uid'] = list(map(lambda x:x.lower(), dfIssue['uid']))
    dfApplication.index = range(dfApplication.shape[0])
    dfApplication['uid'] = list(map(lambda x:x.lower(), dfApplication['uid']))
    dfApplication['hours2017'] = list(map(getHour2017, dfApplication['created_at']))    
    del dfApplication['created_at']
    #***************
    query = """
     SELECT dfIssue.uid, target, hours2017, '''+','.join(variables)+'''
     FROM dfIssue INNER JOIN dfApplication
     ON dfIssue.uid=dfApplication.uid
     """
    dfIssue = ps.sqldf(query,locals())
    #***************    
    flag1 = 0
    flag2 = 0
    len_var = max(list(map(lambda x:len(x), variables)))
    if showLogs:
        print('_' * (len_var + 15))
    for variable in variables:
        time_help = time.time()
        if sum(dfIssue[variable].notnull()) > 0: 
            dfDelta = getDelta_Fraud(
                dfIssue[['uid', 'hours2017', variable]],
                dfApplication[['uid','hours2017', variable]], variable
            )
            #****************************************
            for dayTo in timeIntervals:
                dfFlags = getFlags_Fraud(dfDelta.copy(), dayTo * 24, cntRepeat)
                stat_step = getStatistic_Fraud(
                    dfIssue[['uid', 'target', variable]], dfFlags, dayTo * 24,
                    variable, cntRepeat = cntRepeat
                )
                if flag1 == 0:
                    statistic = stat_step
                    flag1 = 1
                else:
                    statistic = statistic.append(stat_step, ignore_index = True)
            statInt_step = getInterval_Fraud(
                statistic.loc[statistic['variableMain'] == variable].copy()
            )
            if flag2 == 0:
                statisticInterval = statInt_step
                flag2 = 1
            else:
                statisticInterval = statisticInterval.append(statInt_step, ignore_index = True)
        if showLogs:
            print(variable, ' ' * (len_var - len(variable)), ':', round(time.time() - time_help, 2), ' sec.')
    if showLogs:
        print('_' * (len_var + 15))
    #***************
    statistic = getRates(statistic)
    statisticInterval = getRates(statisticInterval)
    if showLogs:
        print(round((time.time() - time_global) / 60, 2), ' min.')
    return statistic, statisticInterval 
    
#******************************************
def Fraud2(dfIssue, dfApplication, variables, timeIntervals, cntRepeat = 1, showLogs = False):
    """
    Calculate statistics for each variables & timeIntervals:
        Issue includes: uid & TARGET
        Application includes uid, created_at, variables
        variables - list из tuple
    """
    #******************************************
    time_global = time.time()
    dfIssue.index = range(dfIssue.shape[0])
    dfIssue['uid'] = list(map(lambda x:x.lower(), dfIssue['uid']))
    dfApplication.index = range(dfApplication.shape[0])
    dfApplication['uid'] = list(map(lambda x:x.lower(), dfApplication['uid']))
    dfApplication['hours2017'] = list(map(getHour2017, dfApplication['created_at']))    
    del dfApplication['created_at']
    varsForIssue = list(dfApplication.columns)
    varsForIssue.remove('uid')
    query = """
     SELECT dfIssue.uid, TARGET, '''+','.join(varsForIssue)+'''
     FROM dfIssue INNER JOIN dfApplication
     ON dfIssue.uid=dfApplication.uid
     """
    dfIssue = ps.sqldf(query, locals())
    del varsForIssue
    
    flag1 = 0
    flag2 = 0
    len_var = max(list(map(lambda x:len(x[0]) + len(x[1]) + 2, variables)))
    if showLogs:
        print('_' * (len_var + 15))
    for variable in variables: 
        time_help = time.time()
        if sum(dfIssue[variable[0]].notnull()) > 0: 
            dfDelta = getDelta_Fraud(
                    dfIssue[['uid', 'hours2017', variable[0], variable[1]]],
                    dfApplication[['uid', 'hours2017', variable[0], variable[1]]],
                    variable[0], variable[1]
            )
            #****************************************
            for dayTo in timeIntervals:
                dfFlags = getFlags_Fraud(dfDelta.copy(), dayTo * 24, cntRepeat)
                stat_step = getStatistic_Fraud(
                    dfIssue[['uid', 'target' , variable[0]]], dfFlags, dayTo * 24,
                    variable[0], variable[1], cntRepeat = cntRepeat
                )                
                if flag1 == 0:
                    statistic = stat_step.copy()
                    flag1 = 1
                else:
                    statistic = statistic.append(stat_step, ignore_index = True)
            statInt_step = getInterval_Fraud(
                statistic.loc[(statistic['variableMain'] ==variable[0]) & (statistic['variableDiff'] == variable[1])].copy()
            )
            if flag2 == 0:
                statisticInterval = statInt_step.copy()
                flag2 = 1
            else:
                statisticInterval = statisticInterval.append(statInt_step, ignore_index = True)
        if showLogs:
            print(variable, ' ' * (len_var - len(variable[0]) - len(variable[1])), ':',
                  round(time.time()-time_help, 2), ' sec.')
    if showLogs:
        print('_' * (len_var + 15))
    #***************
    statistic = getRates(statistic)
    statisticInterval = getRates(statisticInterval)
    if showLogs:
        print(round((time.time() - time_global) / 60, 2), ' min.')
    return statistic, statisticInterval 
#******************************************
def getShortStatistic(df1, df2, fraudType = 1):
    df = df1[
        ['time', 'variableMain', 'variableDiff', 'allCases', 'fraudCases',
        'futureOutMainFraudCases', 'hitRate', 'hitExtRate', 'currentBR', 'fraudBR',
        'futureOutMainBR', 'clearBR', 'benefit_percent', 'benefitClear_percent']
    ]
    df.columns = [
        'time', 'variableMain', 'variableDiff', 'all', 'fraud_1', 'future_1',
        'hitRate_1', 'hitExtRate_1', 'currentBR', 'fraudBR_1', 'futureBR_1',
        'clearBR_1', 'benefit_1', 'benefitClear_1'
    ]
    df.insert(0, 'fraud_2', df2['fraudCases'])
    df.insert(0, 'future_2', df2['futureOutMainFraudCases'])
    df.insert(0, 'hitRate_2', df2['hitRate'])
    df.insert(0, 'hitExtRate_2', df2['hitExtRate'])
    df.insert(0, 'fraudBR_2', df2['fraudBR'])
    df.insert(0, 'futureBR_2', df2['futureOutMainBR'])
    df.insert(0, 'clearBR_2', df2['clearBR'])
    df.insert(0, 'benefit_2', df2['benefit_percent'])
    df.insert(0, 'benefitClear_2', df2['benefitClear_percent'])
    columns = [
        'time','variableMain','variableDiff','all','fraud_1','future_1', 'fraud_2',
        'future_2','hitRate_1','hitExtRate_1','hitRate_2', 'hitExtRate_2', 'currentBR',
        'fraudBR_1','futureBR_1','clearBR_1', 'fraudBR_2','futureBR_2','clearBR_2',
        'benefit_1','benefitClear_1', 'benefit_2','benefitClear_2'
    ]
    if fraudType != 1:
        columns.remove('variableDiff')
    return df[columns]    
#******************************************
def getAndWriteDF(repl, fraudVar, times, country, schemas, where = "status='accepted'"):
    time_global = time.time()
    flag_schema = 0
    varsAppl = ['uid', 'created_at', 'type']
    varsAppl.extend(fraudVar)
    for schema in schemas:
        cols = list((getTableFromMyDB('applications', db = 'scor' + country, 
                        schema=schema, req = '*', where = "", limit = ' limit 1',
                        showTime = False)).columns
        )
        query_vars, _ = getVariables(fraudVar, repl, cols)
        if getTableFromMyDB('application_iovations', db = 'scor' + country, 
                    schema = schema, req = 'count(*)', showTime = False) is None:
            if 'iovation_device_alias' in cols:
                query_vars=query_vars + ', iovation_device_alias'
            dfAppl_step = getTableFromMyDB('applications', db = 'scor' + country,
                        schema=schema, req = query_vars + ', type', where = where,
                        showTime = False
            )
        else:
            query = 'SELECT ' + query_vars + ', device_alias as iovation_device_alias, type FROM '
            query = query + schema + '.applications LEFT JOIN ' + schema
            query = query +'.application_iovations ON applications.id=application_id'
            if where != '':
                query = query + ' WHERE ' + where
            dfAppl_step = getQueryFromMyDB('scor' + country, query, showTime = False)
            del query
        for var in fraudVar:
            if var not in dfAppl_step.columns:
                dfAppl_step[var] = None
        dfAppl_step = dfAppl_step.loc[dfAppl_step['uid'].notnull()]
        if flag_schema == 0:
            dfApplication = dfAppl_step[varsAppl]
            flag_schema = 1
        else:
            dfApplication = dfApplication.append(dfAppl_step[varsAppl], ignore_index = True)
    #***************
    fraudVar2 = []
    for variable in fraudVar:
        if variable not in ['ip', 'iovation_device_alias']:
            fraudVar2.append(('ip', variable))
            fraudVar2.append(('iovation_device_alias', variable))
    fraudVar2.append(('guarantor_fullname', 'full_name'))
    fraudVar2.append(('guarantor_fullname', 'document_number'))
    fraudVar2.append(('guarantor_fullname', 'mobile_phone'))
    fraudVar2.append(('guarantor_fullname', 'email'))
    fraudVar2.append(('guarantor_phone', 'full_name'))
    fraudVar2.append(('guarantor_phone', 'document_number'))
    fraudVar2.append(('guarantor_phone', 'mobile_phone'))
    fraudVar2.append(('guarantor_phone', 'email'))
    #***************
    dfApplication['uid'] = list(map(lambda x:x.lower(), dfApplication['uid']))
    dfApplication.insert(0, 'YW', list(map(lambda x:x.year*100 + x.weekofyear, dfApplication['created_at'])))
    dfApplication.insert(0, 'YD', list(map(lambda x:x.year*1000 + x.dayofyear, dfApplication['created_at'])))
    #***************
    dfIssue = getTableFromMyDB('target', db = 'scor' + country, schema = 'public',
                req = 'uid, target, expdt', where = 'expdt IS NOT NULL', showTime = False)
    dfIssue['expdt'] = list(
        map(lambda x:datetime.datetime.strptime(x,'%Y-%m-%d').date(), dfIssue['expdt'])
    )
    dfIssue = dfIssue.loc[dfIssue['expdt'] <= datetime.date(2017,3,20), ['uid','target']]
    dfIssue['uid'] = list(map(lambda x:x.lower(), dfIssue['uid']))
    print('Data frames was got: ', round((time.time() - time_global) / 60, 2), ' min.')
    print('-' * 20)
    #***************
    for Type in ['Application::Sale', 'Application::Web', 'Application::CreditLine']:
        nameType = Type[13:len(Type)]
        print(nameType)
        query="""
        SELECT dfIssue.uid, target
        FROM dfIssue INNER JOIN dfApplication
        ON dfIssue.uid=dfApplication.uid
        WHERE type=""" + "'" + Type + "'"
        df = ps.sqldf(query, locals())
        print(nameType,':', df.shape)        
        if df.shape[0] > 1000:
            #First fraud-rule
            FirstC_1, First_1 = Fraud1(df.copy(), dfApplication.copy(), fraudVar, times, 1)
            FirstC_2, First_2 = Fraud1(df.copy(), dfApplication.copy(), fraudVar, times, 2)
            fraudOneC = getShortStatistic(FirstC_1, FirstC_2)
            fraudOneC.insert(0, 'statisticType', 'cumulative')
            fraudOne = getShortStatistic(First_1, First_2)
            fraudOne.insert(0, 'statisticType', 'NON-cumulative')
            toSQL = fraudOneC.copy()
            toSQL = toSQL.append(fraudOne, ignore_index = True)
            del FirstC_1, First_1, FirstC_2, First_2,fraudOne, fraudOneC
            #Second fraud-rule
            SecondC_1, Second_1 = Fraud2(df.copy(), dfApplication.copy(), fraudVar2, times, 1)
            SecondC_2, Second_2 = Fraud2(df.copy(), dfApplication.copy(), fraudVar2, times, 2)
            fraudTwoC = getShortStatistic(SecondC_1, SecondC_2)
            fraudTwoC.insert(0, 'statisticType', 'cumulative')
            fraudTwo = getShortStatistic(Second_1, Second_2)
            fraudTwo.insert(0, 'statisticType', 'NON-cumulative')
            toSQL = toSQL.append(fraudTwoC, ignore_index = True)
            toSQL = toSQL.append(fraudTwo, ignore_index = True)
            del SecondC_1, Second_1, SecondC_2, Second_2,fraudTwo, fraudTwoC
            #***************
            insertTableIntoMyDB(toSQL, nameType + '_accepted', db = 'scor' + country,
                    schema = 'temp', showTime = True)
            print('OK! fraud-rules was loaded:', toSQL.shape)
            del df, toSQL
        print(round((time.time() - time_global) / 60, 2), ' min.')
        print()
    #***************
    del dfApplication, dfIssue
    print('-' * 20)
    print(round((time.time() - time_global) / 60, 2), ' min.')

def getLesha(dfApplication, dfIssue, fraudVar1, fraudVar2, times, country):
    time_global = time.time()
    #***************
    flagCountry = 0
    for group in dfApplication['groups'].unique():
        print('group:', group)
        query="""
        SELECT dfIssue.uid, target
        FROM dfIssue INNER JOIN dfApplication
        ON dfIssue.uid=dfApplication.uid
        WHERE groups=""" + "'" + str(group) + "'"
        print(query)
        df = ps.sqldf(query, locals())
        print ('count of rows:', df.shape)        
        #***************
        if df.shape[0] <= 1000:
            print('What the fuck is going on???')
            print('Amount of cases less than 1000')
            print()
        #***************    
        #First fraud-rule
        FirstC_1, First_1 = Fraud1(df.copy(), dfApplication.copy(), fraudVar1, times, 1)
        FirstC_2, First_2 = Fraud1(df.copy(), dfApplication.copy(), fraudVar1, times, 2)
        fraudOneC = getShortStatistic(FirstC_1, FirstC_2)
        fraudOneC.insert(0, 'statisticType', 'cumulative')
        fraudOne = getShortStatistic(First_1, First_2)
        fraudOne.insert(0,'statisticType','NON-cumulative')
        GROUP = fraudOneC.copy()
        GROUP = GROUP.append(fraudOne, ignore_index = True)
        del FirstC_1, First_1, FirstC_2, First_2,fraudOne, fraudOneC
        #Second fraud-rule
        SecondC_1, Second_1 = Fraud2(df.copy(), dfApplication.copy(), fraudVar2, times, 1)
        SecondC_2, Second_2 = Fraud2(df.copy(), dfApplication.copy(), fraudVar2, times, 2)
        fraudTwoC = getShortStatistic(SecondC_1, SecondC_2)
        fraudTwoC.insert(0, 'statisticType', 'cumulative')
        fraudTwo = getShortStatistic(Second_1, Second_2)
        fraudTwo.insert(0, 'statisticType', 'NON-cumulative')
        GROUP = GROUP.append(fraudTwoC, ignore_index = True)
        GROUP = GROUP.append(fraudTwo, ignore_index = True)
        del SecondC_1, Second_1, SecondC_2, Second_2, fraudTwo, fraudTwoC
        #***************
        GROUP['group'] = group
        GROUP['country'] = country
        if flagCountry == 0:
            FINAL = GROUP.copy()
            flagCountry = 1
        else:
            FINAL = FINAL.append(GROUP)
        print('OK! fraud-rules was count:', toSQL.shape)
        del df, GROUP
        print(round((time.time() - time_global) / 60, 2), ' min.')
        print()
    #***************
    del dfApplication, dfIssue
    print('-' * 20)
    print(round((time.time() - time_global) / 60, 2), ' min.')
    return FINAL    
    
