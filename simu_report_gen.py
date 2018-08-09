#coding=utf8
import os
import datetime as dt
import numpy as np
import pandas as pd



from WindPy import w
import cx_Oracle
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def get_all_from_list(filePath=None):
    if filePath is None:
        filePath = r'.\holdLists'
    allLists = os.listdir(filePath)
    allDates = sorted([int(fl.split('.')[0].split('_')[-1]) for fl in allLists])
    cols = ['stkcd','buyDate','ret','flag']
    outPut = {
        'tradeDate': [],
        'buyReturn': [],
        'holdReturn': [],
        'netReturn':[]
    }
    for tdt in allDates:
        retList = pd.read_csv(os.path.join(filePath,'returns_tradeDate_{}.csv'.format(tdt)),encoding='gbk')
        retList = retList.loc[:, cols]
        validBuy = np.all([retList['buyDate']==tdt, np.isin(retList['flag'],(0,3,4))] ,axis=0)
        validHold = np.all([retList['buyDate']<tdt, np.isin(retList['flag'],(0,2,3,4))] ,axis=0)
        outPut['tradeDate'].append(tdt)
        outPut['buyReturn'].append(retList.loc[validBuy,'ret'].mean())
        outPut['holdReturn'].append(retList.loc[validHold,'ret'].mean())
        outPut['netReturn'].append(retList.loc[validBuy | validHold,'ret'].mean())
    outPut = pd.DataFrame(outPut).loc[:,['tradeDate','buyReturn','holdReturn','netReturn']]


def generate_return_report(tradeDate,tradeListPath=None,holdListPath=None):
    tradeDate = str(tradeDate)
    holdListPath = r'.\holdLists' if holdListPath is None else holdListPath
    tradeListPath = r'C:\Users\Administrator\Desktop\simu_report\buyLists' if tradeListPath is None else tradeListPath
    w.start()
    infoDate = w.tdaysoffset(-1,tradeDate).Data[0][0].strftime('%Y%m%d')
    # infoDate = '20180808'
    retList = pd.read_csv(os.path.join(holdListPath,'returns_tradeDate_{}.csv'.format(infoDate)),encoding='gbk')
    trdList = pd.read_csv(os.path.join(tradeListPath,'stkNum50_tradeList_infoDate_{}.csv'.format(infoDate)),encoding='gbk')
    holdStkList = retList.loc[retList['buyDate']==int(infoDate),['stkcd','predictVal']].set_index(['stkcd'])
    buyStkList = trdList.loc[:,['stkcd','predictVal']].set_index(['stkcd'])

    conn = cx_Oracle.connect(r'c##sensegain/sensegain@220.194.41.75/wind')
    cursor = conn.cursor()
    sqlLines = 'SELECT TRADE_DT, S_INFO_WINDCODE, S_DQ_OPEN, S_DQ_CLOSE, S_DQ_PRECLOSE, S_DQ_PCTCHANGE/100, S_DQ_CLOSE/S_DQ_OPEN-1, S_DQ_TRADESTATUS ' \
               'FROM c##wind.AShareEODPrices WHERE TRADE_DT={0}'.format(tradeDate)
    rets = cursor.execute(sqlLines).fetchall()
    rets = pd.DataFrame(rets, columns=['tradeDate', 'stkcd', 'open', 'close', 'preclose', 'retCC','retOC','trdStat'])
    rets['flagOC'] = 1*(rets['trdStat']=='停牌') + 2*(rets['open']/rets['preclose']>=1+0.099) + 3*(rets['open']/rets['preclose']<=1-0.099)
    rets['flagCC'] = 1*(rets['trdStat']=='停牌') + 2*(rets['retCC'] >= 0.099) + 3 * (rets['retCC'] <= -0.099)
    rets['stkcd'] = rets['stkcd'].map(lambda x:int(x[:6]))
    rets.set_index(['stkcd'],inplace=True)
    holdStkList = holdStkList.join(rets.loc[:,['tradeDate','close','retCC','flagCC']])
    holdStkList.columns = ['predictVal','date','close','ret','flag']
    holdStkList['buyDate'] = infoDate
    buyStkList = buyStkList.join(rets.loc[:, ['tradeDate', 'close', 'retOC', 'flagOC']])
    buyStkList.columns = ['predictVal','date','close','ret','flag']
    buyStkList['buyDate'] = tradeDate
    output = pd.concat([holdStkList,buyStkList],axis=0)
    output = output.loc[:,['date','close','ret','flag','buyDate','flag']]
    output.to_csv(os.path.join(holdListPath,'returns_tradeDate_{}.csv'.format(tradeDate)))
    print('return report generated for trade date {}'.format(tradeDate))


def update_recorder(endDate=None,recordFile=None,holdListPath=None):
    endDate = dt.datetime.today().strftime('%Y%m%d') if endDate is None else endDate
    recordFile = r'.\backtest_recorder.csv' if recordFile is None else recordFile
    holdListPath = r'.\holdLists' if holdListPath is None else holdListPath
    recordDF = pd.read_csv(recordFile)
    lastUpdt = recordDF['tradeDate'].values[-1]
    w.start()
    startDate = w.tdaysoffset(1, str(lastUpdt)).Data[0][0].strftime('%Y%m%d')
    betweenDays = w.tdays(startDate,endDate).Data[0]
    betweenDays = [int(tdt.strftime('%Y%m%d')) for tdt in betweenDays]
    print(betweenDays)
    outPut = {
        'tradeDate': [],
        'buyReturn': [],
        'holdReturn': [],
        'netReturn':[]
    }
    cols = ['stkcd', 'buyDate', 'ret', 'flag']
    for tdt in betweenDays:
        retList = pd.read_csv(os.path.join(holdListPath,'returns_tradeDate_{}.csv'.format(tdt)),encoding='gbk')
        retList = retList.loc[:, cols]
        validBuy = np.all([retList['buyDate']==tdt, np.isin(retList['flag'],(0,3,4))] ,axis=0)
        validHold = np.all([retList['buyDate']<tdt, np.isin(retList['flag'],(0,2,3,4))] ,axis=0)
        outPut['tradeDate'].append(tdt)
        outPut['buyReturn'].append(retList.loc[validBuy,'ret'].mean())
        outPut['holdReturn'].append(retList.loc[validHold,'ret'].mean())
        outPut['netReturn'].append(retList.loc[validBuy | validHold,'ret'].mean())
    outPut = pd.DataFrame(outPut).loc[:, ['tradeDate', 'buyReturn', 'holdReturn', 'netReturn']]
    indices = ['000001.SH','000300.SH','000905.SH','000016.SH']
    idxRets = pd.DataFrame(w.wsd(','.join(indices),'pct_chg',startDate,endDate).Data, columns=indices)
    idxRets = idxRets.loc[:]/100
    outPut = pd.concat([outPut, idxRets],axis=1)
    outPut.to_csv(recordFile,mode='a+',index=False,header=False)
    print('recorder updated')


if __name__=='__main__':
    # get_all_from_list()

    # w.start()
    # all = w.wsd('000001.SH,000300.SH,000905.SH,000016.SH','pct_chg','20180601','20180808').Data
    # print(all)

    update_recorder()
    # generate_return_report(20180809)