"""
通过读取数据库和配置文件，生成标签信息
"""
import helper.downloadHelper as dh
import pandas as pd
import numpy as np


class downloadLabel():

    def __init__(self,ip, port, userName, password,database,downPath,type='mysql'):
        self.ssh = dh.downloadDataHelper(ip, port, userName, password,database,downPath,type=type)

    def get_innercode(self, secucodes, secucategory):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :return: string
        """
        innersql = "select InnerCode from SecuMain where SecuCode in (%s) and SecuCategory = %d" % (secucodes, secucategory)
        data = self.ssh.getDataBySQL(innersql)
        innercode = ""
        for index, row in data.iterrows():
            if index == 0:
                innercode = innercode + str(row[0])
            else:
                innercode = innercode + ',' + str(row[0])
        return innercode

    def download_quarter_label(self, secucodes, secucategory, begindate, enddate, secumarket):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :param begindate:string "2016-01-01"
        :param enddate:string "2017-01-01"
        :param secumarket: int 83
        :return:none
        """
        innercodelist = self.get_innercode(secucodes, secucategory)

        zzlsql = "select case when @preVal = qdq.InnerCode then @curVal := (qdq.ClosePrice/@lastVal)-1 when \
                  @preVal := qdq.InnerCode then @lastVal := null and @curVal := (qdq.ClosePrice/@lastVal)-1 end AS zzl,\
                  qdq.TradingDay,qdq.InnerCode,case when @preVal = qdq.InnerCode then @lastVal := qdq.ClosePrice end as temp \
                  from QT_DailyQuote qdq, (select @preVal:=null, @curVal:=null,@lastVal:=null) r where qdq.InnerCode in (%s) \
                  and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and qdq.TradingDay in(select td.TradingDate \
                  from QT_TradingDayNew td WHERE SecuMarket = %d and td.TradingDate BETWEEN '%s 00:00:00' AND '%s 00:00:00' \
                  and td.IfTradingDay = 1 and td.IfQuarterEnd = 1) order by qdq.InnerCode asc,qdq.TradingDay asc" \
                 % (innercodelist, begindate, enddate, secumarket, begindate, enddate)
        self.ssh.downloadDataBySQL(zzlsql, 'label')

    def download_month_label(self, secucodes, secucategory, begindate, enddate, secumarket):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :param begindate:string "2016-01-01"
        :param enddate:string "2017-01-01"
        :param secumarket: int 83
        :return:none
        """
        innercodelist = self.get_innercode(secucodes, secucategory)
        zzlsql = "select case when @preVal = qdq.InnerCode then @curVal := (qdq.ClosePrice/@lastVal)-1 when \
                  @preVal := qdq.InnerCode then @lastVal := null and @curVal := (qdq.ClosePrice/@lastVal)-1 end AS zzl,\
                  qdq.TradingDay,qdq.InnerCode,case when @preVal = qdq.InnerCode then @lastVal := qdq.ClosePrice end as temp \
                  from QT_DailyQuote qdq, (select @preVal:=null, @curVal:=null,@lastVal:=null) r where qdq.InnerCode in (%s) \
                  and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and qdq.TradingDay in(select td.TradingDate \
                  from QT_TradingDayNew td WHERE SecuMarket = %d and td.TradingDate BETWEEN '%s 00:00:00' AND '%s 00:00:00' \
                  and td.IfTradingDay = 1 and td.IfMonthEnd = 1) order by qdq.InnerCode asc,qdq.TradingDay asc" \
                 % (innercodelist, begindate, enddate, secumarket, begindate, enddate)
        self.ssh.downloadDataBySQL(zzlsql, 'label')

    def download_daily_label(self, secucodes, secucategory, begindate, enddate, secumarket):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :param begindate:string "2016-01-01"
        :param enddate:string "2017-01-01"
        :param secumarket: int 83
        :return:none
        """
        innercodelist = self.get_innercode(secucodes, secucategory)
        zzlsql = "select IFNULL(ClosePrice/OpenPrice-1,0) AS zzl,qdq.TradingDay,qdq.InnerCode,0 as temp from QT_DailyQuote qdq \
                  where qdq.InnerCode in (%s) and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and \
                  qdq.TradingDay in(select td.TradingDate from QT_TradingDayNew td WHERE SecuMarket = %d and \
                  td.TradingDate between '%s 00:00:00' and '%s 00:00:00' and td.IfTradingDay = 1) \
                  order by qdq.InnerCode  asc,qdq.TradingDay asc" \
                 % (innercodelist, begindate, enddate, secumarket, begindate, enddate)
        self.ssh.downloadDataBySQL(zzlsql, 'label')

    def get_label_kind(self):
        kindlist = []
        kindlist.append([-999, -15, 'A'])
        kindlist.append([-15, 0, 'B'])
        kindlist.append([0, 15, 'C'])
        kindlist.append([15, 999, 'D'])
        return kindlist

    def get_mod(self, timemod):
        """

        :param timemod: string 'quarter/month/daily'
        :return: int 1,2,3
        """
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
            if (v * 100 >= i[0]) and (v * 100 < i[1]):
                return i[2]
        return ''

    def download_label(self, filepath, secucodes, secucategory, begindate, enddate, labelkinds, secumarket,timemod='quarter'):
        """

        :param filepath: string
        :param secucodes: string('600570','000004')
        :param secucategory: int 1
        :param begindate: string "2016-01-01"
        :param enddate: string "2017-01-01"
        :param timemod: string 'quarter/month/daily'
        :param labelkinds: [[-999, -15, 'A'],[-15, 0, 'B'],[0, 15, 'C'],[15, 999, 'D']]
        :param secumarket: int 83
        :return:none
        """
        mod = self.get_mod(timemod)
        kl = labelkinds
        # dl.download_quarter_label()
        if mod == 1:
            self.download_quarter_label(secucodes, secucategory, begindate, enddate, secumarket)
        elif mod == 2:
            self.download_month_label(secucodes, secucategory, begindate, enddate, secumarket)
        elif mod == 3:
            self.download_daily_label(secucodes, secucategory, begindate, enddate, secumarket)

        data = pd.read_csv(filepath)
        data['label'] = ''
        for item in data['zzl'].index:
            data['label'][item] = self.get_label(data['zzl'][item], kl)
        del data['temp']
        # print(data)
        data.to_csv(filepath, index=False, sep=',')


