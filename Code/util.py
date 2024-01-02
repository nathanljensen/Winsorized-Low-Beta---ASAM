from datetime import date
import pandas as pd
import numpy as np
import datetime as dt
from pandas.tseries.offsets import *
import pickle
import sqlite3

#permno: Apple 14593, Microsoft 10107, Google 90319, IBM 12490, Berkshire Hathaway 17778

#put all dates to end of month
def jdate(df):
    if 'date' in df.columns:
        df['jdate']=pd.to_datetime(df['date'])
        df.drop(columns=['date'], inplace=True)
    return df

def monthEnd(df):
    df=jdate(df)
    df['jdate']=df['jdate']+MonthEnd(0)
    return df

def loadRF(rfpath):
    crsp_rf = pd.read_csv(rfpath,sep=',',skiprows=3)
    crsp_rf.rename(columns={'Unnamed: 0':'date'},inplace=True)
    crsp_rf['date']=pd.to_datetime(crsp_rf['date'])
    return crsp_rf

def loadDelist(dlpath):
    dlret=pickle.load(open(dlpath,'rb'))
    dlret=dlret[['permno','dlret','dlstdt']]
    dlret.permno=dlret.permno.astype(int)
    dlret['date']=pd.to_datetime(dlret['dlstdt'])
    return dlret

def loadSP500(sp5path):
    crsp_sp=pickle.load(open(sp5path,'rb'))
    crsp_sp=crsp_sp.rename(columns={'caldt':'date'})
    crsp_sp['date']=pd.to_datetime(crsp_sp['date'])
    return crsp_sp

def loadCrsp(db):
    crsp_m = db.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, retx, a.shrout, a.prc
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/1920' and '12/31/2020'
                      and b.exchcd between 1 and 3
                      and b.shrcd between 10 and 11
                      """) 
    crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)
    return crsp_m

def loadCrspq(db):
    crsp_m = db.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, retx, a.shrout, a.prc
                      from crspq.msf as a
                      left join crspq.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/1920' and '12/31/2020'
                      and b.exchcd between 1 and 3
                      and b.shrcd between 10 and 11
                      """) 
    crsp_m[['permco','permno','shrcd','exchcd']]=crsp_m[['permco','permno','shrcd','exchcd']].astype(int)
    return crsp_m

parm = {'tickers': (14593, 10107, 84788, 93436, 13407, 14542, 90319, 22111, 55976, 47896, 92611, 91233, 18163, 92655, 26403, 86580, 83443, 66181, 15488, 59408)}

def loadCrspq5(db):
    crsp_m5 = db.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, retx, a.shrout, a.prc
                      from crspq.msf as a
                      left join crspq.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '01/01/1920' and '12/31/2020'
                      and b.exchcd between 1 and 3
                      and b.shrcd between 10 and 11
                      and a.permno in %(tickers)s
                      """, params=parm) 
    crsp_m5[['permco','permno','shrcd','exchcd']]=crsp_m5[['permco','permno','shrcd','exchcd']].astype(int)
    return crsp_m5

def loadCrspd(db):
    crsp_d = db.raw_sql("""
                    select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, retx, a.shrout, a.prc 
                    FROM crsp.dsf a join crsp.dsenames b 
                    on a.permno = b.permno
                    and b.namedt<=a.date
                    and a.date<=b.nameendt
                    where a.date between '01/01/1950' and '12/31/2020'
                    and b.exchcd between 1 and 3
                    and b.shrcd between 10 and 11
                    """)
    crsp_d[['permco','permno','shrcd','exchcd']]=crsp_d[['permco','permno','shrcd','exchcd']].astype(int)
    return crsp_d

parm = {'tickers': (14593, 10107, 84788, 93436, 13407, 14542, 90319, 22111, 55976, 47896, 92611, 91233, 18163, 92655, 26403, 86580, 83443, 66181, 15488, 59408)}

def loadCrspd5(db):
    crsp_d5 = db.raw_sql("""
                    select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, retx, a.shrout, a.prc 
                    FROM crsp.dsf a join crsp.dsenames b 
                    on a.permno = b.permno
                    and b.namedt<=a.date
                    and a.date<=b.nameendt
                    where a.date between '01/01/1950' and '12/31/2020'
                    and b.exchcd between 1 and 3
                    and b.shrcd between 10 and 11
                    and a.permno in %(tickers)s
                    """, params=parm)
    crsp_d5[['permco','permno','shrcd','exchcd']]=crsp_d5[['permco','permno','shrcd','exchcd']].astype(int)
    return crsp_d5

def loadCrspqd(db):
    crsp_d = db.raw_sql("""
                    select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, retx, a.shrout, a.prc 
                    FROM crspq.dsf a join crspq.dsenames b 
                    on a.permno = b.permno
                    and b.namedt<=a.date
                    and a.date<=b.nameendt
                    where a.date between '01/01/1950' and '12/31/2020'
                    and b.exchcd between 1 and 3
                    and b.shrcd between 10 and 11
                    """)
    crsp_d[['permco','permno','shrcd','exchcd']]=crsp_d[['permco','permno','shrcd','exchcd']].astype(int)
    return crsp_d


def maxCap(crsp):
    #Calculate market equity
    crsp['me']=crsp['prc'].abs()*crsp['shrout']

    #Remove unnecessary columns and sort
    crsp=crsp.drop(['dlret','dlstdt','prc','shrout'], axis=1)
    crsp=crsp.sort_values(by=['jdate','permco','me'])

    #Sum of market equity across different permno (securities) belonging to same permco (company) a given date
    crsp_summe = crsp.groupby(['jdate','permco'])['me'].sum().reset_index()

    #Dataframe of largest mktcap within a permco/date
    crsp_maxme = crsp.groupby(['jdate','permco'])['me'].max().reset_index()

    #Join by jdate and max market equity to find the permno
    crsp1=pd.merge(crsp, crsp_maxme, how='inner', on=['jdate','permco','me'])

    #Drop me column and join with sum of market equity to get the correct market cap info for the company as a whole
    crsp1=crsp1.drop(['me'], axis=1)
    crsp2=pd.merge(crsp1, crsp_summe, how='inner', on=['jdate','permco'])

    #Sort by permno and date and also drop duplicates
    crsp=crsp2.sort_values(by=['permno','jdate']).drop_duplicates()
    return crsp

#create lag based on datetime increment specified 'day' 'month' 'year' etc)
#truncates DF of months 1-11 if by year, days if by month, etc.
def lag(df,sig):
    df=df.sort_values(by=['jdate','permno'])
    df['lag'+sig]=df.groupby(['permno'])[sig].shift(1)
    return df

#take everything after converting CRSP and dlret date time to pandas format
#and put it in here for combining ret and dlret
def comebineRet(crsp,dlret):
    #merge delisted into listed
    crsp = pd.merge(crsp, dlret, how='left',on=['permno','jdate'])
    crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1
    crsp['retxadj']=(1+crsp['retx'])*(1+crsp['dlret'])-1

    #Clear N/A return entries and calculated returns adjusted for the delisting return
    crsp['dlret']=crsp['dlret'].fillna(0)
    crsp['ret']=crsp['ret'].fillna(0)
    crsp['retx']=crsp['retx'].fillna(0)

    #combine returns
    crsp['retadj']=(1+crsp['ret'])*(1+crsp['dlret'])-1
    crsp['retxadj']=(1+crsp['retx'])*(1+crsp['dlret'])-1
    return crsp

#trim df to top X stocks based on signal, grouped by jdate
#ideally run this after time period of interest is chosen
#e.g. if jdate is monthly it will pick out top n stocks for every month, 
#but if you want annual then trim the dataset to annual first, which the util.lag function 
#will do implicitly
def topX(dfinput, tsig,nstocks):
    df=dfinput.copy()
    #rank all stocks based on signal 'tsig', e.g. market cap or beta etc at given time
    df[tsig+'Rank']=df.groupby(['jdate'])[tsig].rank(ascending=False)
    #drop all stocks higher rank than that signal value 
    df.drop(df.loc[df[tsig+'Rank']>nstocks].index,inplace=True)
    df=df.drop([tsig+'Rank'],axis=1)
    return df

def botX(dfinput, tsig,nstocks):
    df=dfinput.copy()
    #rank all stocks based on signal 'tsig', e.g. market cap or beta etc at given time
    df[tsig+'Rank']=df.groupby(['jdate'])[tsig].rank(ascending=True)
    #drop all stocks higher rank than that signal value 
    df.drop(df.loc[df[tsig+'Rank']>nstocks].index,inplace=True)
    df=df.drop([tsig+'Rank'],axis=1)
    return df


#calculate annual return for dataframe, assuming monthly returns given
def annRet(df):
    #Create annual return data set
    df=df.sort_values(['permno','jdate'],ascending=[True,True])
    df['jdate'] = pd.to_datetime(df['jdate'])
    df['month'] = df['jdate'].dt.month
    df['year'] = df['jdate'].dt.year
    df['1+retadj']=df['retadj']+1
    df['annRet'] = df.groupby(['permno','year'])['1+retadj'].cumprod()-1
    return df

#split into nquant quantiles, on a per jdate basis
def splitQ(dfinput,qsig,nquant):
    df = dfinput.copy()
    df.sort_values([qsig,'jdate'])

    #calc n quantiles by the desired signal 'sig'
    df['rank'+qsig]=df.groupby(['jdate'])[qsig].rank(ascending=True)
    df[qsig+'q'] = pd.qcut(df['rank'+qsig],nquant,labels=False)
    return df

#function for calcating returns based on value weighted portfolio
#assuming jdate is the only date increment of interest, will rebalance per jdate increm.
#weighting is basically one stock per permno.
#also takes number of quantile cuts as nquant, and which signal to qcut as 'qsig'
#RETURN:  dataframe port with jdate, value weighted (vw) return, returnx, cumul return, cumul return x as:
#vwret, vwretx, vwcret, vwcretx
def vw(df,qsig,nquant):

    df.sort_values([qsig,'jdate'])

    #calc n quantiles by the desired signal 'sig'
    df['rank']=df.groupby(['jdate'])[qsig].rank(ascending=False)
    df[qsig+'quantile'] = pd.qcut(df['rank'],nquant,labels=False)

    #weight ret and retx based on market cap of permno for value weighted stocks from previous time period
    df['wret']=df['retadj']*df['lagme']
    #df['wretx']=df['retxadj']*df['lagme']

    #portfolio return is sum of the value weighted returns per quantile
    port=df.groupby([qsig+'quantile','jdate'])['wret'].sum().reset_index(name='vwret')
    #portx=df.groupby([qsig+'quantile','jdate'])['wretx'].sum().reset_index(name='vwretx')
    #divide by the market cap of that quantile to get normalized return
    portnorm=df.groupby([qsig+'quantile','jdate'])['lagme'].sum().reset_index()

    port['vwret']=port['vwret']/portnorm['lagme']
    #portx['vwretx']=portx['vwretx']/portnorm['lagme']

    #port=pd.merge(port,portx,how='inner',on=[qsig+'quantile','jdate'])
    port['vwcret']=port['vwret']+1
    #port['vwcretx']=port['vwretx']+1
    port['vwcret']=port.groupby([qsig+'quantile'])['vwcret'].cumprod()
    #port['vwcretx']=port.groupby([qsig+'quantile'])['vwcretx'].cumprod()

    return port

#winsorize signal wsig based on refsig and delta
def winsorize(df,wsig,refsig,delta):
    df['4'+refsig]=df[refsig]*(1+delta)
    df['2'+refsig]=df[refsig]*(1-delta)
    df['max'+refsig]=df[['4'+refsig,'2'+refsig]].max(axis=1)
    df['min'+refsig]=df[['4'+refsig,'2'+refsig]].min(axis=1)
    df['w'+wsig]=df[[wsig,'max'+refsig]].min(axis=1)
    df['w'+wsig]=df[['w'+wsig,'min'+refsig]].max(axis=1)
    df.drop(columns=['2'+refsig,'4'+refsig,'min'+refsig,'max'+refsig],inplace=True)
    return df

#function for calcating returns based on equal weighted portfolio
#assuming jdate is the only date increment of interest, will rebalance per jdate increm.
#also takes number of quantile cuts as nquant, and which signal to qcut as 'sig'
#RETURN:  dataframe port with jdate, equal weighted (ew) return, returnx, cumul return, cumul return x as:
#ewret, ewretx, ewcret, ewcretx
def ew(df,qsig,nquant):

    df.sort_values([qsig,'jdate'])

    #calc n quantiles by the desired signal 'sig'
    df['rank']=df.groupby(['jdate'])[qsig].rank(ascending=False)
    df[qsig+'quantile'] = pd.qcut(df['rank'],nquant,labels=False)

    #portfolio return is mean of the equal weighted returns per quantile
    port=df.groupby([qsig+'quantile','jdate'])['retadj'].mean().reset_index(name='ewret')
    #portx=df.groupby([qsig+'quantile','jdate'])['retxadj'].mean().reset_index(name='ewretx')

    #port=pd.merge(port,portx,how='inner',on=[qsig+'quantile','jdate'])
    port['ewcret']=port['ewret']+1
    #port['ewcretx']=port['ewretx']+1
    port['ewcret']=port.groupby([qsig+'quantile'])['ewcret'].cumprod()
    #port['ewcretx']=port.groupby([qsig+'quantile'])['ewcretx'].cumprod()
    return port

#get operating profit from compustat
#'op' is profit in cash, 'opSig' is total book asset deflated profit as signal
def op(comp):
    comp['op'] = comp['revt']-comp['cogs']-(comp['xsga']-comp['xrd'])
    comp['opSig']=comp['op']/comp['at']
    return comp
