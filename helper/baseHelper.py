import configparser
import os
import logging
import calendar as cale
import time
import pandas as pd
import tushare as ts
from datetime import datetime,timedelta




"""
    配置文件帮助类
    功能：加载配置文件返回配置文件中的信息
"""
class configHelper():
    # 初始化配置文件
    def __init__(self,path='../chooseStock.cfg'):
        self.conf = configparser.ConfigParser()
        if os.path.exists(path):
            self.conf.read(path, encoding='utf-8')
        else:
            print('没有找到配置文件')
            return

    # 获取配置文件信息
    def getConfig(self,section,option):
        # self.basepath = self.conf.get('FILE_PATH', 'basepath')
        # self.tradeDaysFN = self.basepath + self.conf.get('FILE_NAME', 'tradeDaysFN')
        return self.conf.get(section, option)

"""
    日志帮助类
    功能：封装日志的基本功能
"""
class logHelper():

    def __init__(self):
        cfg = configHelper()
        logName = cfg.getConfig('LOG','logName')
        filemode = cfg.getConfig('LOG','writeModel')
        level = cfg.getConfig('LOG','level')
        logging.basicConfig(level=self.getLevel(level),
                            format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%a, %d %b %Y %H:%M:%S',
                            filename=logName,
                            filemode=filemode)
        # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
    def getLevel(self,num):
        return {
            '1': logging.ERROR,
            '2': logging.WARNING,
            '3': logging.INFO,
            '4': logging.DEBUG,
            '5': logging.NOTSET
        }.get(num, logging.DEBUG)

    def logMsg(self,level,msg):
        return {
            '1': logging.error(msg),
            '2': logging.warning(msg),
            '3': logging.info(msg),
            '4': logging.debug(msg),
        }.get(level)



"""
    文件路径帮助类
    包含了对路径的操作
"""
class filePathHelper():

    # 获取所有的文件绝对路径
    @staticmethod
    def listFilesPath(path,containDir = False):
        fileNameList = []
        if containDir:
            for file in os.listdir(path):
                file_path = os.path.join(path, file)
                fileNameList.append(file_path)
        else:
            for file in os.listdir(path):
                file_path = os.path.join(path, file)
                if not os.path.isdir(file_path):
                    fileNameList.append(file_path)
        return fileNameList





"""
    日期帮助类
    包括了对交易日和日期的一些操作    
"""
class tradeDayHelper(object):
    # 获取某年某月中最后一天
    # 入参：
    #       year：年  month：月
    # 出参：
    #       yyyy-MM-dd
    @staticmethod
    def getLastDayOnMonth(year, month):
        # monthrange获取某年某月的起始星期和天数
        day = cale.monthrange(year, month)[1]
        date = str(year) + '-' + str(month) + '-' + str(day)
        # strptime时间按照格式解析成时间数组
        timeArray = time.strptime(date, "%Y-%m-%d")
        # strftime将时间数组转化为其他格式
        return time.strftime("%Y-%m-%d", timeArray)

    # 获取某年某些月中最后一天
    # 入参：
    #       year：年(int)  months：月份(list)
    # 出参：
    #       yyyy-MM-dd(list)
    @staticmethod
    def getLastDayOnMonths(self, year, months):
        dateList = []
        for month in months:
            dateList.append(self.getLastDayOnMonth(year, month))
        return dateList

    # 获取某年中所有季度最后一天的日期
    # 入参：
    #       year：年
    # 出参：
    #       yyyy-MM-dd
    def getLastDayOnQuarter(self,year):
        return self.getLastDayOnMonths(year, [3, 6, 9, 12])

    # df = getAllTreadeDay('')
    # isTradeDay('',df)

    # 对日期的天数进行操作
    # 入参：
    #       date：初始日期   dayNum:操作天数 加一天1  减一天-1
    # 出参
    #       操作后的日期yyyy-MM-dd
    def operateDay(date, dayNum):
        dateArray = datetime.strptime(date, "%Y-%m-%d")
        dateArray = dateArray + timedelta(days=dayNum)
        return dateArray.strftime("%Y-%m-%d")
    # 获取所有的交易日
    def getAllTreadeDay(self,dataFileName):
        if os.path.exists(dataFileName):
            df = pd.read_csv(dataFileName)
        else:
            df = ts.trade_cal()
            df.to_csv(dataFileName)
        tradeDays = df[df.isOpen == 1]['calendarDate'].values
        return tradeDays

    def __init__(self):
        self.getConfig()
        self.tradeDays = self.getAllTreadeDay(self.tradeDaysFN)


    # 判断当前日期是否是交易日
    # 入参：
    #   date：yyyy-MM-dd
    # 出参：
    #   Boolean：是交易日(True)  不是交易日(False)
    def isTradeDay(self, date):
        if isinstance(date, str):
            today = datetime.strptime(date, '%Y-%m-%d')
        if today.isoweekday() in [6, 7] or str(date) not in self.tradeDays:
            return False
        else:
            return True


    # 获取当前日期附近前的交易日，如果当前是交易日则直接返回
    # 入参：
    #       date:操作日期   tradeDays：交易日
    def getNearTradeDay(self, date, num = -1):
        if not self.isTradeDay(date):
            date = self.operateDay(date, num)
            return self.getNearTradeDay(date, self.tradeDays)
        else:
            return date

    # 获取月份最后一个交易日
    # 入参：
    #   year：年(int)  months:月(list)
    # 出参：
    #   月份最后一个交易日(list)
    def getLastTradeDay(self, year, months):
        lastDays = self.getLastDayOnMonths(year, months)
        lastTradeDays = []
        # tradeDays = getAllTreadeDay('tradeDay.csv')
        # for lastDay in lastDays:
        #     if isTradeDay(lastDay, tradeDays):
        #         lastTradeDays.append(lastDay)
        #     else:
        #         pass

        pass

# tradeDayHelper = tradeDayHelper()
# print(tradeDayHelper.getNearTradeDay('2017-05-31'))