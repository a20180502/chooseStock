"""
通过读取数据库和配置文件，生成标签信息
"""
import helper.downloadHelper as dh
import pandas as pd
import numpy as np

class downloadLabel():
    def get_innercode(self):
        secucode = "'000004','600570'"
        innersql = "select InnerCode from SecuMain where SecuCode in (%s) and SecuCategory = 1" % (secucode)
        ssh = dh.downloadDataHelper()
        data = ssh.getDataBySQL(innersql)
        innercode = ""
        for index, row in data.iterrows():
            if index == 0:
                innercode = innercode + str(row[0])
            else:
                innercode = innercode + ',' + str(row[0])
        return innercode

    def download_quarter_label(self):
        innercodelist = self.get_innercode()
        begindate = "2016-01-01"
        enddate = "2017-01-01"
        zzlsql = "select case when @preVal = qdq.InnerCode then @curVal := (qdq.ClosePrice/@lastVal)-1 when \
                  @preVal := qdq.InnerCode then @lastVal := null and @curVal := (qdq.ClosePrice/@lastVal)-1 end AS zzl,\
                  qdq.TradingDay,qdq.InnerCode,case when @preVal = qdq.InnerCode then @lastVal := qdq.ClosePrice end as temp \
                  from QT_DailyQuote qdq, (select @preVal:=null, @curVal:=null,@lastVal:=null) r where qdq.InnerCode in (%s) \
                  and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and qdq.TradingDay in(select td.TradingDate \
                  from QT_TradingDayNew td WHERE SecuMarket = 83 and td.TradingDate BETWEEN '%s 00:00:00' AND '%s 00:00:00' \
                  and td.IfTradingDay = 1 and td.IfQuarterEnd = 1) order by qdq.InnerCode asc,qdq.TradingDay asc" \
                 % (innercodelist, begindate, enddate, begindate, enddate)
        ssh = dh.downloadDataHelper()
        ssh.downloadDataBySQL(zzlsql, 'label')

    def download_month_label(self):
        innercodelist = self.get_innercode()
        begindate = "2016-01-01"
        enddate = "2017-01-01"
        zzlsql = "select case when @preVal = qdq.InnerCode then @curVal := (qdq.ClosePrice/@lastVal)-1 when \
                  @preVal := qdq.InnerCode then @lastVal := null and @curVal := (qdq.ClosePrice/@lastVal)-1 end AS zzl,\
                  qdq.TradingDay,qdq.InnerCode,case when @preVal = qdq.InnerCode then @lastVal := qdq.ClosePrice end as temp \
                  from QT_DailyQuote qdq, (select @preVal:=null, @curVal:=null,@lastVal:=null) r where qdq.InnerCode in (%s) \
                  and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and qdq.TradingDay in(select td.TradingDate \
                  from QT_TradingDayNew td WHERE SecuMarket = 83 and td.TradingDate BETWEEN '%s 00:00:00' AND '%s 00:00:00' \
                  and td.IfTradingDay = 1 and td.IfMonthEnd = 1) order by qdq.InnerCode asc,qdq.TradingDay asc" \
                 % (innercodelist, begindate, enddate, begindate, enddate)
        ssh = dh.downloadDataHelper()
        ssh.downloadDataBySQL(zzlsql, 'label')

    def download_daily_label(self):
        innercodelist = self.get_innercode()
        begindate = "2016-01-01"
        enddate = "2017-01-01"
        zzlsql = "select IFNULL(ClosePrice/OpenPrice-1,0) AS zzl,qdq.TradingDay,qdq.InnerCode,0 as temp from QT_DailyQuote qdq \
                  where qdq.InnerCode in (%s) and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and \
                  qdq.TradingDay in(select td.TradingDate from QT_TradingDayNew td WHERE SecuMarket = 83 and \
                  td.TradingDate between '%s 00:00:00' and '%s 00:00:00' and td.IfTradingDay = 1) \
                  ORDER BY INNERCODE,TradingDay" \
                 % (innercodelist, begindate, enddate, begindate, enddate)
        ssh = dh.downloadDataHelper()
        ssh.downloadDataBySQL(zzlsql, 'label')

    def get_label_kind(self):
        kindlist = []
        kindlist.append([-999, -15, 'A'])
        kindlist.append([-15, 0, 'B'])
        kindlist.append([0, 15, 'C'])
        kindlist.append([15, 999, 'D'])
        return kindlist

    def get_mod(self):
        timemod = 'quarter'
        if timemod == 'quarter':
            return 1
        elif timemod == 'month':
            return 2
        elif timemod == 'daily':
            return 3

    def get_label(self, value, kindlist):
        v = value
        if np.isnan(v):
            v = np.float64(0)
        for i in kindlist:
            if v * 100 >= i[0] and v * 100 < i[1]:
                return i[2]
        return ''


if __name__ == '__main__':
    dl = downloadLabel()
    mod = dl.get_mod()
    kl = dl.get_label_kind()
    #dl.download_quarter_label()
    if mod == 1:
        dl.download_quarter_label()
    elif mod == 2:
        dl.download_month_label()
    elif mod == 3:
        dl.download_daily_label()

    data = pd.read_csv('C:\\Users\\fujl13237\\PycharmProjects\\chooseStock\\DATA\\download\\label.csv')

    """
        for item in data['zzl']:
            print(dl.get_label(item,kl))
            item = dl.get_label(item,kl)
    """
    data['label'] = ''
    for item in data['zzl'].index:
        data['label'][item] = dl.get_label(data['zzl'][item], kl)
    del data['temp']
    #print(data)
    data.to_csv("C:\\Users\\fujl13237\\PycharmProjects\\chooseStock\\DATA\\download\\label.csv", index=False, sep=',')

