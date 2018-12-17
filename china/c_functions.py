# -*- coding: utf-8 -*-
"""
Created on Wed May 17 15:58:54 2017

@author: aleks
"""

"""DATA PROCESSING"""
"""substr in str"""
def asubstr(arr,line):
    for substr in arr:
        if substr in line.lower():
            return True
    return False

def getInfo(df,train_test_YW,minInGroup=100,n_notForBin=5):
    info_df=pd.DataFrame({'col_name':df.columns, 'type_in_data':df.dtypes,'group':'','group_sup':'',
    'comment':'', 'num_of_uniq':0,'cnt_big_groups':0,'null_prop':0.,'null_train':0.,'null_test':0.,
    'not_null_start':0, 'not_null_finish':0,'num_of_unique_test':0,'IV':0})
    #смотрим доли null в каждом месяце
    for mon in df.YM.unique():
        info_df.insert(1,'null_'+str(mon),0)

    for col in df.columns:
        #null & not null
        info_df.loc[info_df['col_name']==col,'null_prop']=round(100.*df[col].isnull().sum()/df.shape[0],2)
        info_df.loc[info_df['col_name']==col,'null_train']=round(100.*sum((df[col].isnull()) & (df.YW<=train_test_YW))/sum(df.YW<=train_test_YW),2)
        info_df.loc[info_df['col_name']==col,'null_test']=round(100.*sum((df[col].isnull()) & (df.YW>train_test_YW))/sum(df.YW>train_test_YW),2)
        info_df.loc[info_df['col_name']==col,'num_of_unique_test']=len(df.loc[df.YW>train_test_YW,col].unique())

        info_df.loc[info_df['col_name']==col,'cnt_big_groups']=sum(df.loc[(df[col].notnull()) & (df['YW']<=train_test_YW),col].value_counts()>=minInGroup)
        if df[col].isnull().sum()>0:
            for mon in df['YM'].unique():
                info_df.loc[info_df['col_name']==col,'null_'+str(mon)]=round(100.*df[col][df.YM==mon].isnull().sum()/sum(df.YM==mon),2)
        ar_YW=df.YW[df[col].notnull()].values
        if len(ar_YW)>0:
            info_df.loc[info_df['col_name']==col,'not_null_start']=min(ar_YW)
            info_df.loc[info_df['col_name']==col,'not_null_finish']=max(ar_YW)
        #uniques
        cnt=len(df.loc[df[col].notnull(),col].unique())
        info_df.loc[info_df['col_name']==col,'num_of_uniq']=cnt
        #groups:location, ids, phones, codes
        cond=(info_df['col_name']==col) & (info_df['num_of_uniq']+(info_df['null_prop']<100)>1)
        if asubstr(['village','city','living','location','region','country'],col):
            info_df.loc[info_df['col_name']==col,'group']='location'
        if (asubstr(['id'],col)) & (sum(cond & (info_df['group']==''))==1):
            info_df.loc[info_df['col_name']==col,'group']='ids'
        if (asubstr(['phone'],col)) & (sum(cond & (info_df['group']==''))==1):
            info_df.loc[info_df['col_name']==col,'group']='phones'
        if (asubstr(['code'],col)) & (sum(cond & (info_df['group']==''))==1):
            info_df.loc[info_df['col_name']==col,'group']='code'
        if (asubstr(['code'],col)) & (sum(cond & (info_df['group']=='phones'))==1):
            info_df.loc[info_df['col_name']==col,'group']='code'
            info_df.loc[info_df['col_name']==col,'comment']='phone_code'        
    #Группы
#    info_df.loc[(info_df['cnt_big_groups']+(info_df['null_prop']<100)>1) & (info_df['cnt_big_groups']<=n_notForBin) & (info_df['num_of_uniq']<=200),'group']='notForBin'
    info_df.loc[(info_df['cnt_big_groups']>0) & (info_df['cnt_big_groups']<=n_notForBin),'group']='notForBin'
    
    cond_numeric=(info_df['num_of_uniq']>1) & ((info_df['type_in_data']=='int64') | (info_df['type_in_data']=='float64')) & (info_df['group']=='')
    info_df.loc[cond_numeric,'group']='numeric'

    info_df.loc[(info_df['type_in_data']=='<M8[ns]'),'group']='datetimes'
    info_df.loc[(info_df['group']=='') & (info_df['num_of_uniq']>1),'group']='na'
    info_df.loc[(info_df['group']=='na') & (info_df['cnt_big_groups']>n_notForBin) & (info_df['cnt_big_groups']<100),'group']='nominal'
    return info_df
"""Проверка на дубли"""
def getCols(group1,group2,info_df):
    comparison=[]
    vars2=info_df.loc[info_df.group==group2,'col_name']    
    for var1 in info_df.loc[info_df.group==group1,'col_name']:
        vars2=vars2[vars2!=var1]
        for var2 in vars2:
            un1=info_df.loc[info_df.col_name==var1,'num_of_uniq'].values[0]
            un2=info_df.loc[info_df.col_name==var2,'num_of_uniq'].values[0]
            if min(un1,un2)<6:
                cond=abs(un1-un2)<2
            elif min(un1,un2)<21:
                cond=abs(un1-un2)<3
            elif min(un1,un2)<500:
                cond=abs(un1-un2)<4
            else:
                cond=False
            if cond & (sum((info_df.group=='copy') & ((info_df.col_name==var1) | (info_df.col_name==var2)))==0):
                comparison.append((var1,var2))
    return comparison 
def bool_copy_percent(var1,var2):
    if (var1.dtype=='int64') | (var1.dtype=='float64'):
        na1=-1000000
    else:
        na1='null'
    if (var2.dtype=='int64') | (var2.dtype=='float64'):
        na2=-1000000
    else:
        na2='null'
    c_na1=na1 in set(var1.values)
    c_na2=na2 in set(var2.values)
    #***********************
    if c_na1 & c_na2:
        cond=(var1!=na1) & (var2!=na2)
    elif c_na1 & (not c_na2):
        cond=(var1!=na1)
    elif (not c_na1) & c_na2:
        cond=(var2!=na2)
    else:
        cond=list(range(len(var1)))
    #***********************
    if 100.*sum(cond)/len(var1)<10:
        return 'different'
    var1_na=var1[cond]
    var2_na=var2[cond]
    uni1=set(var1_na.values)
    uni2=set(var2_na.values)  
    if len(uni1)!=len(uni2):
        return 'different'

    for val in uni1:
        if len(set(var2_na[var1_na==val].values))>1:
            return 'different'

    """одинаковые..."""
    cond1=set(var1[list(map(lambda x:not x,cond))])==set([na1])
    cond2=set(var2[list(map(lambda x:not x,cond))])==set([na2])        

    if (cond[3] not in set([True,False])) | (cond1 & cond2):
        return (0,'full '+str(round(100.*len(var1_na)/len(var1),2))+'% ')
    
    if (not cond1) & cond2:
        info=str(round(100.*sum(var1!=na1)/len(var1),2) if c_na1 else 100.00)
        return (0,'nested set '+info+'% ')
    if cond1 & (not cond2):
        info=str(round(100.*len(var2!=na1)/len(var2),2) if c_na2 else 100.00)
        return (1,'nested set '+info+'% ')
    if (not cond1) & (not cond2):
        info1=str(round(100.*sum((var1!=na1) & (var2!=na2))/len(var1),2))
        info2=str(round(100.*sum((var1!=na1) | (var2!=na2))/len(var1),2))
        return (2,'complement'+info1+'% '+info2+'% ')




"""BINNING"""
"""common functions"""
def C_nk(n,k):
    if n-k<k:
        k=n-k
    out=1
    for i in range(k):
        out=out*(n-i)
    return int(out/math.factorial(k))

def WOE(p,n,P,N):
    p_delta=p/P
    n_delta=n/N
    if p_delta==0:
        return -100
    elif n_delta==0:
        return 100
    else:
        return math.log1p(p_delta/n_delta-1)
        
        
"""variables that have less 2-5 values"""      
#ordinary
def n_groups(var,n,min_in_group,na=-1000000):
    """Разбиение на группы перед биннингом"""
    u_values=var.unique()
    u_values.sort()
    u_values=u_values[u_values!=na]
    var_act=pd.DataFrame({'var':var,'group':0})
    #count of groups is less (or equal to) than necessary 
    if len(u_values)<=n:
        #Если есть null, он отойдет к группе "0"
        for i in range(len(u_values)):
            var_act.loc[var_act['var']==u_values[i],'group']=i+1
        return del_smallGroups(var_act,min_in_group)    
    #count of groups is greater than necessary
    var_act_notnull=var_act[var_act['var']!=na]
    l=len(var_act['var'])
    var_act_notnull.sort_values('var',inplace=True,ascending=True)
    var_act_notnull.insert(2,'index_new',range(l))    
    #var_act=pd.DataFrame({'var':var.sort_values(inplace=False,ascending=True),'group':0,'index_new':range(l)}) 
    if sum(var_act['var']==na)>0:
        n=n-1

    while sum((var_act_notnull['group']==0))>0:
#        print(sum((var_act_notnull['group']==0)))
        gr_number=int(var_act_notnull['group'].max()+1)
        cond0=var_act_notnull['group']==0
        if sum(cond0)<=l/n:
            var_act_notnull.loc[cond0,'group']=gr_number
            break
        
        index_from= min(var_act_notnull.loc[cond0,'index_new'])
        index_to=index_from+l//n-1 #1 зачем отнимаем в случае округления вниз?
#        print(index_from,index_to)
        board1=(var_act_notnull.loc[var_act_notnull['index_new']==index_from,'var']).values[0]
        board2=(var_act_notnull.loc[var_act_notnull['index_new']==index_to,'var']).values[0]
        if board1==board2:
            board=board1
        else:
            board1=max(u_values[u_values<board2])
            cond1=(cond0) & (var_act_notnull['var']<=board1)
            cond2=(cond0) & (var_act_notnull['var']<=board2)
            if (l/n-sum(cond1)<=sum(cond2)-l/n) & sum(cond0 & (var_act_notnull['var']<=board1))>=min_in_group:
                board=board1
            else:
                board=board2
        if sum(cond0 & (var_act_notnull['var']>board))<min_in_group:
            var_act_notnull.loc[cond0,'group']=gr_number
            break
        else:
            var_act_notnull.loc[cond0 & (var_act_notnull['var']<=board),'group']=gr_number
    var_act_notnull.drop('index_new', axis=1, inplace=True)
    if sum(var_act['var']==na)>0:
        var_act_notnull.append(var_act[var_act['var']==na])
    return var_act_notnull.sort_index()

def del_smallGroups(df,min_in_group):
    i=1
    flag=True
    while True:
          
        if (sum(df.group==i)>0) & (sum(df.group==i)<min_in_group):
            flag=False
            n_less=i
            n_greater=i
            if sum((df.group>0) & (df.group<i))>0:
                n_less=df.loc[df.group<i,'group'].max()
            if sum(df.group>i)>0:
                n_greater=df.loc[df.group>i,'group'].min()
            if n_less==i:
                df.loc[df.group==i,'group']=n_greater
            elif n_greater==i:
                df.loc[df.group==i,'group']=n_less
            elif sum(df.group==n_less)<sum(df.group==n_greater):
                df.loc[df.group==i,'group']=n_less
            else:
                df.loc[df.group==i,'group']=n_greater
        if (sum(df.group>i)==0) & (flag):
            break
        elif (sum(df.group>i)==0) & (not flag):
            i=1
            flag=True
        else:
            i=i+1
    sort_unique=df.group.unique()
    sort_unique.sort()
    if sort_unique[0]>1:
        df.loc[df.group==sort_unique[0],'group']=1
        sort_unique[0]=1
    for group in sort_unique[1:]:
        df.loc[df.group==group,'group']=df.loc[df.group<group,'group'].max()+1
    return df        

def IV_boards(df,boards):
    woe=[]
    IV=[]
    p_arr=[]
    n_arr=[]
    badRate=[]
    P=df['target'].values.sum()
    N=len(df['target'])-P
    if  sum(df['group']==0)>0:
        p=sum(df.loc[df['group']==0,'target'].values)
        n=sum(df['group']==0)-p
        woe_step=WOE(p,n,P,N)
        woe.append(woe_step)
        IV.append((p/P-n/N)*woe_step)
        p_arr.append(p)
        n_arr.append(n)
        badRate.append(round(p/(p+n),2))
    for i in range(len(boards)):
        cond=(df['group']>0) & (df['group']<=boards[i])
        if i>0:
            cond=cond & (df['group']>boards[i-1])
        p=sum(df.loc[cond,'target'].values)
        n=sum(cond)-p
        woe_step=WOE(p,n,P,N)        
        woe.append(woe_step)
        IV.append((p/P-n/N)*woe_step)
        p_arr.append(p)
        n_arr.append(n)
        badRate.append(round(p/(p+n),2))
#    return (woe,IV,p_arr,n_arr,badRate)
    return (np.array(woe),np.array(IV),p_arr,n_arr,badRate)
    
"""Получение woe для разных видов переменных"""
def woeFast(var,y,typeOfGroup='woeFast',minInGroup=100):
    df=pd.DataFrame({'variable':var,'group':var,'target':y})    
    uniq_cnt=df.loc[df['variable']!='null','variable'].value_counts()
    #присваиваем df['group']='smallGroup' для тех значений, которых <=minInGroup
    if sum(uniq_cnt<minInGroup)>0:
        values=uniq_cnt.loc[uniq_cnt<minInGroup].index
        for value in values:
            df.loc[df['variable']==value,'group']='smallGroup'    
    #******************           
    df_stat=pd.DataFrame({'variable':'','value':df.variable.unique(),'bottomBorder':df.variable.unique(),'topBorder':'','allBad':0,'allGood':0,'badRate':0,'woe':0,'IV':0,'typeOfGroup':typeOfGroup})
    if sum(df.group=='smallGroup')>0:
        for value in df.loc[df['group']=='smallGroup','variable']:
            df_stat.loc[df_stat['value']==value,'bottomBorder']='smallGroup'
    #Заполняем df_stat
    P=y.values.sum()
    N=len(y.values)-P
    groups=df['group'].unique()
    for group in groups:
        p=sum(y.loc[df['group']==group].values)
        n=sum(df['group']==group)-p
        #*********************        
        df_stat.loc[df_stat['bottomBorder']==group,'allBad']=n
        df_stat.loc[df_stat['bottomBorder']==group,'allGood']=p        
        df_stat.loc[df_stat['bottomBorder']==group,'badRate']=round(n/(p+n),2)
        if (p+n)>=minInGroup:#возможно, за искл. "мелких" групп 
            woe=WOE(p,n,P,N)
            df_stat.loc[df_stat['bottomBorder']==group,'woe']=woe
            df_stat.loc[df_stat['bottomBorder']==group,'IV']=(p/P-n/N)*woe        
    #создаем и заполняем privot_stat
    pivot_stat=pd.DataFrame({'variable':'','type':'some values','bottomBorder':0,'topBorder':0,'value':'','allBad_train':0,'allGood_train':0,'badRate_train':0,'allBad_test':0,'allGood_test':0,'badRate_test':0,'woe':df_stat['woe'].unique(),'IV':0})
    for woe in pivot_stat.woe.values:
        pivot_stat.loc[pivot_stat.woe==woe,'allBad_train']=df_stat.loc[df_stat['woe']==woe,'allBad'].unique()[0]
        pivot_stat.loc[pivot_stat.woe==woe,'allGood_train']=df_stat.loc[df_stat['woe']==woe,'allGood'].unique()[0]
        pivot_stat.loc[pivot_stat.woe==woe,'badRate_train']=df_stat.loc[df_stat['woe']==woe,'badRate'].unique()[0]
        pivot_stat.loc[pivot_stat.woe==woe,'IV']=df_stat.loc[df_stat['woe']==woe,'IV'].unique()[0]
        pivot_stat.loc[pivot_stat.woe==woe,'value']=','.join(df_stat.loc[df_stat['woe']==woe,'value'].unique().astype(str))
    return (df_stat[['variable','value','typeOfGroup','bottomBorder','topBorder','woe']],pivot_stat)

def woeNumeric(var,y,n_bins=5,n_original=50,min_in_group=100,na=-1000000):
    #Первичное разбиение на группы, проверка на необходимость разбиения предусмотрена в "n_groups".
    df=n_groups(var,n_original,min_in_group,na) 
    df.insert(1,'target',y)
    #сравнение кол-ва уникальных значений с кол-вом групп в разбиении
    n_gr=df.group.max()
    if n_gr-(sum(df.group==0)>0)<n_bins:
        n_bins=n_gr-(sum(df.group==0)>0)
    #инициализируем разделительные перегородки (их на 1 меньше, чем групп), ищем наилучшее разбиение
    boards=np.arange(1,n_bins)
    boards[n_bins-2]=n_bins-2
    flag=n_bins-2
    max_IV=0
#    print(n_gr,n_bins,boards)
    for i in range(C_nk(n_gr-1,n_bins-1)):
        boards[flag]=boards[flag]+1
        if flag<(n_bins-2):
            for j in range(flag+1,n_bins-1):
                boards[j]=boards[j-1]+1
        for j in range(n_bins-2,-1,-1):
            if boards[j]<n_gr-1-(n_bins-2-j):
                flag=j
                break
        woe_step, IV_step,p_step,n_step,badRate_step=IV_boards(df[['group','target']],np.append(boards,n_gr))
        if sum(IV_step)>max_IV:
            max_IV=sum(IV_step)
            info=(np.append(boards,n_gr),woe_step,IV_step,p_step,n_step,badRate_step) 
#        print(i,'-',boards)
#    return (1,2)
    #расчитываем df_stat
    b_values=[] 
    if sum(df['group']==0)>0:
        b_values.append(df.loc[df.group==0,'var'].max())
        if info[3][0]+info[4][0]<min_in_group:
            info[1][0]=0
            info[2][0]=0
    for i in range(n_bins):
        b_values.append(df.loc[df.group==info[0][i],'var'].max())
    
    for i in range(len(info[3])):
        if info[3][i]+info[4][i]<min_in_group:
            info[1][i]=0
            info[2][i]=0
    pivot_stat=pd.DataFrame({'variable':'','type':'(bottom;upper]','bottomBorder':np.append(-float('Inf'),b_values[:-1]),'topBorder':np.append(b_values[:-1],float('Inf')),'value':'','allBad_train':info[4],'allGood_train':info[3],'badRate_train':info[5],'allBad_test':0,'allGood_test':0,'badRate_test':0,'woe':info[1],'IV':info[2]})
    df_stat=pd.DataFrame({'variable':'','value':'','typeOfGroup':'woeNumeric','bottomBorder':pivot_stat['bottomBorder'],'topBorder':pivot_stat['topBorder'],'woe':pivot_stat['woe']})
    return (df_stat,pivot_stat)

def woeNominal(var,y,n_bins=5,min_in_group=100):
    u_values=list(var.value_counts()[var.value_counts()>=min_in_group].index)    
    df=pd.DataFrame({'variable':var,'group':var,'defolt':0, 'woe':0, 'IV':0,'allBad':0,'allGood':0,'badRate':0})
    df.loc[list(map(lambda x: x not in u_values,df['variable'])),'group']='smallGroup'
    u_values=df['group'].unique()
    #такого быть не должно априори
    if sum((u_values!='smallGroup') & (u_values!='null'))<=n_bins:
        print('You are good, but... FAST WOE in NOMINAL WOE')
        return woeFast(df['variable'],y,'nominalFast',min_in_group)
    #количество "больших" групп больше n_bins
    for value in u_values[u_values!='smallGroup']:
        df.loc[df['variable']==value,'defolt']=sum((df['variable']==value) & (y==0))/sum(df['variable']==value)
    #null(или не используемые) обозначаем "принятым" значением, заменяющим null
    df.loc[df['group']=='smallGroup','defolt']=sum(y==0)/len(y)
    info_stat,info_pivot=woeNumeric(df['defolt'],y,n_bins,min_in_group=min_in_group)
    for i in range(info_pivot.shape[0]):
        df.loc[(df['defolt']>info_pivot.bottomBorder[i]) & (df['defolt']<=info_pivot.topBorder[i]),'woe']=info_pivot.woe[i]
        df.loc[(df['defolt']>info_pivot.bottomBorder[i]) & (df['defolt']<=info_pivot.topBorder[i]),'IV']=info_pivot.IV[i]
        df.loc[(df['defolt']>info_pivot.bottomBorder[i]) & (df['defolt']<=info_pivot.topBorder[i]),'allBad']=info_pivot.allBad_train[i]
        df.loc[(df['defolt']>info_pivot.bottomBorder[i]) & (df['defolt']<=info_pivot.topBorder[i]),'allGood']=info_pivot.allGood_train[i]
        df.loc[(df['defolt']>info_pivot.bottomBorder[i]) & (df['defolt']<=info_pivot.topBorder[i]),'badRate']=info_pivot.badRate_train[i]
    #*********************************
    df_stat=pd.DataFrame({'variable':'','value':df['variable'].unique(),'bottomBorder':df['variable'].unique(),'topBorder':'','allBad':0,'allGood':0,'badRate':0,'woe':0,'IV':0,'typeOfGroup':'woeNominal'})
    if sum(df['group']=='smallGroup')>0:
        for value in df.loc[df['group']=='smallGroup','variable']:
            df_stat.loc[df_stat['value']==value,'bottomBorder']='smallGroup'
    #*********************************
    for group in df.loc[df['group']!='smallGroup','group']:
        temp=df.loc[df['group']==group,['woe','IV','allBad','allGood','badRate']]
        temp.index=list(range(temp.shape[0]))
        df_stat.loc[df_stat['bottomBorder']==group,'woe']=temp.woe[0]
        df_stat.loc[df_stat['bottomBorder']==group,'IV']=temp.IV[0]
        df_stat.loc[df_stat['bottomBorder']==group,'allBad']=temp.allBad[0]
        df_stat.loc[df_stat['bottomBorder']==group,'allGood']=temp.allGood[0]
        df_stat.loc[df_stat['bottomBorder']==group,'badRate']=temp.badRate[0]
    #*********************************
    pivot_stat=pd.DataFrame({'variable':'','type':'some values','bottomBorder':0,'topBorder':0,'value':'','allBad_train':0,'allGood_train':0,'badRate_train':0,'allBad_test':0,'allGood_test':0,'badRate_test':0,'woe':df_stat['woe'].unique(),'IV':0})
    for woe in pivot_stat['woe'].values:
        pivot_stat.loc[pivot_stat['woe']==woe,'allBad_train']=df_stat.loc[df_stat['woe']==woe,'allBad'].unique()[0]
        pivot_stat.loc[pivot_stat['woe']==woe,'allGood_train']=df_stat.loc[df_stat['woe']==woe,'allGood'].unique()[0]
        pivot_stat.loc[pivot_stat['woe']==woe,'badRate_train']=df_stat.loc[df_stat['woe']==woe,'badRate'].unique()[0]
        pivot_stat.loc[pivot_stat['woe']==woe,'IV']=df_stat.loc[df_stat['woe']==woe,'IV'].unique()[0]
        pivot_stat.loc[pivot_stat['woe']==woe,'value']=','.join(df_stat.loc[df_stat['woe']==woe,'value'].unique().astype(str))
    return (df_stat[['variable','value','typeOfGroup','bottomBorder','topBorder','woe']],pivot_stat)

    
def colToWoe(df_train,df_test,info,typeOfOper,naInclude=True):
    if typeOfOper=='woeNumeric':
        na=-1000000
    else:
        na='null'
    cond_train=[]
    cond_test=[]
    info.index=range(info.shape[0])

    if typeOfOper=='woeFast':
        for i in range(info.shape[0]):
            cond_train.append(df_train==info.bottomBorder[i])
            cond_test.append(df_test==info.bottomBorder[i])
    if typeOfOper=='woeNumeric':
        for i in range(info.shape[0]):
            cond_train.append((df_train.astype(float)<=float(info.topBorder[i])) & (df_train.astype(float)>float(info.bottomBorder[i])))
            cond_test.append((df_test.astype(float)<=float(info.topBorder[i])) & (df_test.astype(float)>float(info.bottomBorder[i])))
    if typeOfOper=='woeNominal':
        for i in range(info.shape[0]):
            cond_train.append(df_train==info.bottomBorder[i])
            cond_test.append(df_test==info.bottomBorder[i])
#            if info.bottomBorder[i]==na:
#            cond_train[i]=pd.Series(map(lambda x: sum(info.bottomBorder[:-1]==x)==0,df_train))
#            cond_test[i]=pd.Series(map(lambda x: sum(info.bottomBorder[:-1]==x)==0,df_test))
    #**********************
    out_train=pd.Series([0]*len(df_train),index=df_train.index)
    out_test=pd.Series([0]*len(df_test),index=df_test.index)
    for i in range(len(cond_train)):
        out_train[cond_train[i].values]=info.woe[i]
        out_test[cond_test[i].values]=info.woe[i]
    return (out_train,out_test)

    
def markSmallGroups(var,minInGroups):
    df=pd.DataFrame({'variable':var.astype('object'),'group':''})
    df.loc[df.variable=='-1000000']='null'
    for value in df.variable.unique():
        if sum(df.variable==value)<minInGroup:
            df.loc[df.variable==value,'group']='small_groups'
        else:
            df.loc[df.variable==value,'group']=value
    return df
"""Стабильность переменных"""