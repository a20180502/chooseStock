"""
通过读取数据库和配置文件，生成标签信息
"""
import helper.downloadHelper as dh
import dataCollection.downloadStockPool as dsm
import pandas as pd
import numpy as np


class downloadStockLabel():

    def __init__(self,downloadDataHelper):
        self.ssh = downloadDataHelper
        self.sm = sm = dsm.downloadStockPool(downloadDataHelper)

    def _get_innercode(self, secucodes, secucategory):
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

    def _download_quarter_label(self, secucodes, secucategory, begindate, enddate, secumarket, labefilename):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :param begindate:string "2016-01-01"
        :param enddate:string "2017-01-01"
        :param secumarket: int 83
        :return:none
        """
        if secucodes is  None:
            innercodelist = '-1'
        else:
            innercodelist = self._get_innercode(secucodes, secucategory)

        zzlsql = "select * from(SELECT case when @preVal = mm.InnerCode then @curVal := (mm.ClosePrice/@lastVal)-1 \
                when @preVal := mm.InnerCode then @lastVal := null and @curVal := (mm.ClosePrice/@lastVal)-1 end AS zzl,\
                mm.InnerCode,mm.TradingDay,case when @preVal = mm.InnerCode then @lastVal := mm.ClosePrice end as temp \
                FROM(SELECT	qdq.InnerCode,qdq.TradingDay,qdq.ClosePrice FROM QT_DailyQuote qdq WHERE \
                ((qdq.InnerCode in (%s) and '%s' != '-1') or ('%s' = '-1')) and \
                qdq.TradingDay BETWEEN date_add('%s 00:00:00', INTERVAL - 2 DAY)AND '%s 00:00:00' AND \
                qdq.TradingDay IN(SELECT td.TradingDate FROM QT_TradingDayNew td WHERE SecuMarket = %d AND td.TradingDate \
                BETWEEN date_add( '%s 00:00:00', INTERVAL - 2 DAY) AND '%s 00:00:00' AND td.IfTradingDay = 1  \
                AND td.IfQuarterEnd = 1) ORDER BY qdq.InnerCode ASC,qdq.TradingDay ASC) mm,(select @preVal:=null, @curVal:=null,\
                @lastVal:=null) r)nn where nn.zzl is not NULL" \
                 % (innercodelist, innercodelist, innercodelist, begindate, enddate, secumarket, begindate, enddate)
        print(zzlsql)
        self.ssh.downloadDataBySQL(zzlsql, labefilename)

    def _download_month_label(self, secucodes, secucategory, begindate, enddate, secumarket, labefilename):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :param begindate:string "2016-01-01"
        :param enddate:string "2017-01-01"
        :param secumarket: int 83
        :return:none
        """
        if secucodes is None:
            innercodelist = '-1'
        else:
            innercodelist = self._get_innercode(secucodes, secucategory)

        zzlsql = "select * from(SELECT case when @preVal = mm.InnerCode then @curVal := (mm.ClosePrice/@lastVal)-1 \
                when @preVal := mm.InnerCode then @lastVal := null and @curVal := (mm.ClosePrice/@lastVal)-1 end AS zzl,\
                mm.InnerCode,mm.TradingDay,case when @preVal = mm.InnerCode then @lastVal := mm.ClosePrice end as temp \
                FROM(SELECT	qdq.InnerCode,qdq.TradingDay,qdq.ClosePrice FROM QT_DailyQuote qdq WHERE \
                ((qdq.InnerCode in (%s) and '%s' != '-1') or ('%s' = '-1')) and \
                qdq.TradingDay BETWEEN date_add('%s 00:00:00', INTERVAL - 2 DAY)AND '%s 00:00:00' AND \
                qdq.TradingDay IN(SELECT td.TradingDate FROM QT_TradingDayNew td WHERE SecuMarket = %d AND td.TradingDate \
                BETWEEN date_add( '%s 00:00:00', INTERVAL - 2 DAY) AND '%s 00:00:00' AND td.IfTradingDay = 1  \
                AND td.IfQuarterEnd = 1) ORDER BY qdq.InnerCode ASC,qdq.TradingDay ASC) mm,(select @preVal:=null, @curVal:=null,\
                @lastVal:=null) r)nn where nn.zzl is not NULL" \
                % (innercodelist, innercodelist, innercodelist, begindate, enddate, secumarket, begindate, enddate)
        self.ssh.downloadDataBySQL(zzlsql, labefilename)

    def _download_daily_label(self, secucodes, secucategory, begindate, enddate, secumarket, labefilename):
        """

        :param secucodes:string "'600570','000004'"
        :param secucategory:int 1
        :param begindate:string "2016-01-01"
        :param enddate:string "2017-01-01"
        :param secumarket: int 83
        :return:none
        """
        if secucodes is None:
            innercodelist = '-1'
        else:
            innercodelist = self._get_innercode(secucodes, secucategory)

        zzlsql = "select IFNULL(ClosePrice/OpenPrice-1,0) AS zzl,qdq.TradingDay,qdq.InnerCode,0 as temp from QT_DailyQuote qdq \
                  where ((qdq.InnerCode in (%s) and '%s' != '-1') or ('%s' = '-1'))\
                  and qdq.TradingDay between '%s 00:00:00' and '%s 00:00:00' and \
                  qdq.TradingDay in(select td.TradingDate from QT_TradingDayNew td WHERE SecuMarket = %d and \
                  td.TradingDate between '%s 00:00:00' and '%s 00:00:00' and td.IfTradingDay = 1) \
                  order by qdq.InnerCode  asc,qdq.TradingDay asc" \
                 % (innercodelist, innercodelist, innercodelist, begindate, enddate, secumarket, begindate, enddate)
        print(zzlsql)
        self.ssh.downloadDataBySQL(zzlsql, labefilename)

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

    def set_label(self,filepath,labefilename,kl):
        data = pd.read_csv(filepath + '\\' + labefilename + '.csv')
        data['label'] = ''
        i = 0
        for item in data['zzl'].index:
            i = i+1
            print(i)
            data['label'][item] = self.get_label(data['zzl'][item], kl)
        del data['temp']
        data.to_csv(filepath + '\\' + labefilename + '.csv', index=False, sep=',')

    def download_label(self, filepath, secucodes, secucategory, begindate, enddate, labelkinds, secumarket, labefilename, timemod='quarter'):
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
        if mod == 1:
            self._download_quarter_label(secucodes, secucategory, begindate, enddate, secumarket, labefilename)
        elif mod == 2:
            self._download_month_label(secucodes, secucategory, begindate, enddate, secumarket, labefilename)
        elif mod == 3:
            self._download_daily_label(secucodes, secucategory, begindate, enddate, secumarket, labefilename)

        data = pd.read_csv(filepath + '\\' + labefilename + '.csv')
        print('read data finish')
        lt = np.array(data).tolist()
        for i in lt:
            i.append(self.get_label(i[1], kl))
        df = pd.DataFrame(lt)
        df.rename(columns={0: 'id', 1: 'zzl', 2: 'InnerCode', 3: 'TradinsDay', 4: 'temp', 5: 'label'}, inplace=True)
        del df['temp']
        # print(data)
        df.to_csv(filepath + '\\' + labefilename + '.csv', index=False, sep=',')

    def download(self, stockpoolpath, downloadfilepath ,labelkinds, begindate, enddate, labelfilename, timemod='quarter'):
        pooldata = self.sm.readstockpool(stockpoolpath)
        Id = pooldata[0]
        Name = pooldata[1]
        Market = int(pooldata[2])
        Type = int(pooldata[3])
        Industry = pooldata[4]
        Plate = pooldata[5]
        Hasst = pooldata[6]
        Savepath = pooldata[7]
        labelkinds = self.get_label_kind()
        self.download_label(downloadfilepath, None, Type, begindate, enddate, labelkinds, Market, labelfilename, timemod)


# if __name__ == '__main__':
#     tt = downloadLabel('10.20.34.12',3306,'ziguan','ziguan@123','gildata_test',r'C:\Users\fujl13237\PycharmProjects\chooseStock','mysql')
#     tt.download(r'C:\Users\fujl13237\PycharmProjects\chooseStock\DATA\stockPool.csv',
#                 r'C:\Users\fujl13237\PycharmProjects\chooseStock', None, '2016-01-01', '2017-01-01', 'label', 'quarter')
