# -*- coding: utf-8 -*-
"""
Created on Mon May 22 11:55:35 2017

@author: aleks
"""

def getTableFromDB(typeConnection='infinto', db='doctordong_production',schema='public',tableName='applications',req='*',where='',limit=0,showQuery=False,showLogs=False):
    timeFunc=time.time()
    try:
        if typeConnection=='infinto':
            server = SSHTunnelForwarder(('119.81.95.210', 22), ssh_password='Xy4n9kf9Yl0N', ssh_username='infinto', remote_bind_address=('127.0.0.1', 5432))
            server.start()
            if showLogs:
                print('SSH-tunnel is ok')
            conn = psycopg2.connect(database=db, user='infinto', password='Ozk1BPOjknTz', host='127.0.0.1' , port=server.local_bind_port)
        elif typeConnection=='myDB':
            conn = psycopg2.connect(database=db, user='postgres', password='qmzpqm', host='localhost' , port=5432)
        else:
            print ('ERROR!!!: You can use only "infinto" or "myDB" typeConnection')
            return
        if showLogs:
            print('conn was created')
        whereFunc=''
        limitFunc=''
        if where!='':
            whereFunc=' WHERE '+where
        if limit>0:
            limitFunc=' LIMIT '+str(limit)
        #***************
        query='SELECT '+req+' FROM '+schema+'.'+tableName+whereFunc+limitFunc
        if showQuery:
            print(query)
        df=pd.read_sql(query,conn)
        if showLogs:
            print('df was loaded')
            print('-'*45)
            print('Working time: ',round(time.time()-timeFunc,2),' sec.')
        return df
    except:
        if showLogs:
            print('ERROR!!!: You have a problem with loading')
            print('-'*45)
            print('Working time: ',round(time.time()-timeFunc,2),' sec.')
        return
    finally:
        if typeConnection=='infinto':
            if server is not None:
                server.stop()
        if conn is not None:
            conn.close()

def deleteDataFromTable(db='scorVietnam',schema='public',tableName='applications',where='',showQuery=False,showLogs=False):
    try:
        conn = psycopg2.connect(database=db, user='postgres', password='qmzpqm', host='localhost' , port=5432)
        cur=conn.cursor()
        if where=='':
            query="DELETE FROM "+schema+"."+tableName
        else:
            query="DELETE FROM "+schema+"."+tableName+" WHERE "+where
        if showQuery:
            print(query)            
        cur.execute(query)        
        print('deleted: ',cur.rowcount)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print('error:',error)
    finally:
        if conn is not None:
            conn.close()

            
def getTableFromInfinto(tableName,req='*',limit='',dataBase='doctordong_production', where='',showTime=False):
    time_help=time.time()
    try:
        server = SSHTunnelForwarder(('119.81.95.210', 22), ssh_password='Xy4n9kf9Yl0N', ssh_username='infinto', remote_bind_address=('127.0.0.1', 5432))
        server.start()                 

        conn_CN = psycopg2.connect(database=dataBase, user='infinto', password='Ozk1BPOjknTz', host='127.0.0.1' , port=server.local_bind_port)
        df=pd.read_sql('select '+req+' from '+tableName+where+limit , conn_CN)
        if showTime:
            print(round(time.time()-time_help,2))
        return df
    except sshtunnel.BaseSSHTunnelForwarderError: 
        print ('You have a problem with ssh tunnel')
    
'doctordong.vn'
def getTableFromMyDB(tableName='applications',db='scorVietnam',schema='doctordong',req='*',where='',limit='',showTime=True,showQuery=False):
    time_help=time.time()
    try:  
        conn = psycopg2.connect(database=db, user='postgres', password='qmzpqm', host='localhost' , port=5432)
        if where=='':
            query='select '+req+' from '+schema+'.'+tableName+limit
        else:
            query='select '+req+' from '+schema+'.'+tableName+' WHERE '+where+' '+limit
        if showQuery:
            print(query)
        df=pd.read_sql(query,conn)
        
        if showTime:
            print('getTableFromMyDB. Well done!')
            print('Working time: ',round(time.time()-time_help,2),' sec.')
        return df
    except:
        if showTime:
            print('getTableFromMyDB. Error with load.')
            print('Working time: ',round(time.time()-time_help,2),' sec.')
            


def getQueryFromMyDB(db='scorChina',query='*',showTime=True):
    time_help=time.time()
    try:  
        conn = psycopg2.connect(database=db, user='postgres', password='qmzpqm', host='localhost' , port=5432)
        df=pd.read_sql(query , conn)
        if showTime:
            print('getQueryFromMyDB. Well done!')
            print('Working time: ',round(time.time()-time_help,2),' sec.')
        return df
    except:
        if showTime:
            print('getQueryFromMyDB. Error with load.')
            print('Working time: ',round(time.time()-time_help,2),' sec.')


       
def insertTableIntoMyDB(df,tableName,db='scorChina',schema='public',showLogs=False):
    time_help=time.time()  
    infoDf=getTypesOfColumn(df)
    jsons=infoDf.loc[infoDf['types']==dict,'colName'].values
    lists=infoDf.loc[infoDf['types']==list,'colName']
    if showLogs:
        print('json: ',jsons)
        print('list: ',lists)
    types=dict.fromkeys(jsons,JSON)
    types.update(dict.fromkeys(lists,''))
    for curList in lists:
        l=infoDf.loc[infoDf['colName']==curList,'typeInList'].values[0]
        if type(l)!=int:
            l=255
        types[curList]=ARRAY(CHAR(l))
    engine = create_engine('postgresql://postgres:qmzpqm@localhost:5432/'+db)
    
    df.index=list(range(df.shape[0]))
    l=math.ceil(df.shape[0]/10000)
    if showLogs:
        print('load was started')
    for i in range(l):
        time_step=time.time()
        df[(10000*i):min(10000*(i+1),df.shape[0])].to_sql(tableName,engine,index=False,schema=schema,if_exists='append',dtype=types)
        if showLogs:
            print(i+1,'-',l,':',round(time.time()-time_step,2),' sec.')    
    if showLogs:
        print('inserted:',sd.shape[0])
        print(round((time.time()-time_help)/60,2),' min.')
 