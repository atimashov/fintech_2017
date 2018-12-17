# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 23:21:05 2017

@author: Саша
"""

def updateDataFromCompany(info, daysDeleted = 7):
    for i in range(info.shape[0]):
        print(colored([info.loc[i, 'dbMy'], info.loc[i, 'schemaMy']], 'green'))
        print('-' * 25)
        print('-' * 25)
        #applications
        print('applications')
        createDate = getTableFromDB(
                typeConnection = 'myDB', db = info.loc[i, 'dbMy'], schema = info.loc[i,'schemaMy'],
                req = 'date(created_at) as date', showQuery = False, showLogs = False
        )['date'].value_counts()
        createDate = list(createDate[createDate > 10].index)
        createDate.sort()        
        if len(createDate) >= daysDeleted:
            dateFrom = createDate[-daysDeleted]
            print('dateFrom:', dateFrom)
            deleteDataFromTable(
                db = info.loc[i,'dbMy'], schema = info.loc[i, 'schemaMy'],
                where = "date(created_at)>='" + dateFrom.strftime('%Y-%m-%d') + "'",
                showQuery = False
            )
            appId = getTableFromDB(
                typeConnection = 'myDB', db = info.loc[i,'dbMy'], schema = info.loc[i,'schemaMy'],
                req = 'max(id) as id', showQuery = False, showLogs = False
            ).loc[0, 'id']
            addData = getTableFromDB(
                typeConnection = os.environ['mf_name'], db = info.loc[i, 'schemaMy'] + '_production',
                req = '*', where = "date(created_at)>='" + dateFrom.strftime('%Y-%m-%d') + "'",
                showQuery = False, showLogs = False
            )
            insertTableIntoMyDB(
                addData, 'applications', db = info.loc[i,'dbMy'], schema = info.loc[i,'schemaMy']
            )
        else:
            appId = 0
            print('Too small table')
            deleteDataFromTable(
                db = info.loc[i, 'dbMy'], schema = info.loc[i, 'schemaMy'], 
                where = '', showQuery = False
            )
            addData = getTableFromDB(
                typeConnection = os.environ['mf_name'], db = info.loc[i, 'schemaMy'] + '_production',
                req = '*', where = '', showQuery = False, showLogs = False)
            insertTableIntoMyDB(
                addData, 'applications', db = info.loc[i, 'dbMy'], schema = info.loc[i,'schemaMy']
            )
        print('+' * 20)
        
        #application_iovations
        print('application_iovations:')
        colsFrom = getTableFromDB(
            typeConnection = os.environ['mf_name'], tableName = 'application_iovations', 
            db = info.loc[i,'schemaMy'] + '_production', req = '*', limit = 1,
            showQuery = False, showLogs = False
        )
        colsTo = getTableFromDB(
            typeConnection = 'myDB', tableName = 'application_iovations',
            db = info.loc[i, 'dbMy'], schema = info.loc[i, 'schemaMy'], req = '*',
            limit = 1, showQuery = False, showLogs = False
        )
        #***************
        if colsFrom is not None:
            if colsTo is None:
                print("I haven't table yet.")
                addData = getTableFromDB(
                    typeConnection = os.environ['mf_name'], db=info.loc[i, 'schemaMy'] + '_production',
                    tableName = 'application_iovations', req = '*', where = '',
                    showQuery = False, showLogs = False
                )
                insertTableIntoMyDB(
                    addData, 'application_iovations', db = info.loc[i,'dbMy'],
                    schema = info.loc[i, 'schemaMy']
                )
            elif set(colsTo.columns) == set(colsFrom.columns):
                print('Tables are the same.')
                deleteDataFromTable(
                    db = info.loc[i, 'dbMy'], schema = info.loc[i, 'schemaMy'],
                    tableName = 'application_iovations', where = "application_id>" + str(appId),
                    showQuery = False
                )
                addData = getTableFromDB(
                    typeConnection = os.environ['mf_name'], db = info.loc[i, 'schemaMy'] + '_production',
                    tableName = 'application_iovations', req = '*', 
                    where = 'application_id>' + str(appId), showQuery = False, showLogs = False
                )
                insertTableIntoMyDB(
                    addData, 'application_iovations', db = info.loc[i, 'dbMy'],
                    schema = info.loc[i, 'schemaMy']
                )
            else:
                print('ERROR: Some table has been changed.')
        elif colsTo is not None:
            print('ERROR: Table was deleted.')    
        print()
    
    #delete data from...

def updateTarget(countries):
    """
    countries includes 'Vietnam', 'Indonesia','Malaysia','Philippiness'
    """
    timeFunc=time.time()
    #***************
    try:
        conn=pyodbc.connect("""Driver={SQL Server};SERVER=10.64.188.144;DATABASE=RH_Risk;UID=aleksei.pogrebnyak;PWD=Ziskelevich009;""")
        test=pd.read_sql("select top 1 * from PHApplication", conn )
        cursor=conn.cursor()
        cursor.execute('select top 1 * into [RH_RISK].[AP].[testx] from vnapplication')
        cursor.commit()
        cursor.execute('drop table [RH_RISK].[AP].[testx]')
        cursor.commit()
        cursor.close()
    except pyodbc.Error: 
        print(colored("Подключение к БД 'target' выдало ошибку",'red'))
        return
    #***************
    for country in countries:
        print("-"*25)
        print(country)
        flag=0
        if country=='Vietnam':
            var='VN'
        elif country=='Indonesia':
            var='ID'
        elif country=='Malaysia':
            var='MY'
        elif country=='Philippiness':
            var='PH'
        else:
            flag=1
            print(colored("New country or incorrect country. Check it!",'red'))
        if flag==0:
            #***************
            queryTarget = """
            select 
            '"""+var+"""' as country,
            CC.TSPortalID as uid,
            BB.TSSignedOn as signdt,
            BB.TSValidFrom as validfromdt,
            BB.TSExpiresOn as expdt,
            MAX(AA.dpd) AS maxdpd,
            IIF(MAX(AA.dpd)>=10,1,0) AS target
            --INTO [RH_RISK].[AP].[PH_TARGET_I]
            FROM [RH_RISK].[dbo].["""+var.lower()+"""_hron_ost] AA 
            inner JOIN [RH_RISK].[dbo].["""+var+"""Agreement] BB ON AA.agr_id = BB.ID
            iNNER JOIN [RH_RISK].[dbo].["""+var+"""AppLICATION] CC ON BB.TSApplicationID = CC.ID
            INNER JOIN [RH_RISK].[dbo].["""+var+"""AppGATE] DD ON CC.TSPortalID = DD.TSPortalID
            WHERE CC.TSPortalID  IS NOT NULL and CC.TSPortalID  <> ''
            GROUP BY 
            CC.TSPortalID,
            BB.TSSignedOn,
            BB.TSExpiresOn,
            BB.TSValidFrom
            """
            TARGET=pd.read_sql(queryTarget, conn)
            TARGET['updated_at']=datetime.date.today()
            print(TARGET.shape)
            queryUids="('"+"','".join(list(TARGET['uid'].values))+"')"
            print(getTableFromDB(typeConnection='myDB', db='scor'+country,schema='public',tableName='target',req='count(*)',where='',limit=0,showQuery=False,showLogs=False).loc[0,'count'])
            deleteDataFromTable(db='scor'+country,schema='public',tableName='target',where='uid IN '+queryUids,showQuery=False,showLogs=False)
            insertTableIntoMyDB(TARGET,'target',db='scor'+country,schema='public',showLogs=False)
            print(round(time.time()-timeFunc,2),' sec.')


