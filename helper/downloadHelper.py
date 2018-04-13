import pandas as pd
from helper.baseHelper import configHelper
from sqlalchemy import create_engine
"""
    数据下载帮助类
        主要进行连接不同的数据库,从数据库读取数据并且保存到本地
        @author 张朝丰
"""
# 加载本单元所需要的配置文件
configHelper = configHelper()

"""
    SQL基础帮助类
"""
class sqlBaseHelper():
    def _getConnect_(self):
        raise NotImplementedError

    def __init__(self, ip, port, userName, password, database):
        # self.connectInfo = pymysql.connect(ip,userName,password,database)
        # self.cursor = self.connectInfo.cursor()
        self.engine = None
        self.ip = ip
        self.port = port
        self.userName = userName
        self.password = password
        self.database = database
        self._getConnect_()

    # 根据表名\字段名\日期范围\证券代码从数据库中获取数据
    def getDataByTable(self, table, columns=None, dateRange=None, stockList=None):
        print("正在读取数据库表", table, '字段', columns)
        df = pd.read_sql_table(table, self.engine, columns=columns)
        print("读取数据库表完成", table)
        return df

    # 根据SQL从数据库中获取数据
    def getDataBySQL(self, sqlstr):
        print("正在执行SQL,读取数据:", sqlstr)
        df = pd.read_sql(sqlstr, self.engine)
        print("执行SQL完毕,读取数据描述:")
        print(df.describe())
        return df


"""
    Oracle帮助类
"""
class oracleHelper(sqlBaseHelper):

    # 需要更改为连接Oracle的连接串
    def _getConnect_(self):
        print("正在尝试连接数据库")
        conInfo = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(self.userName, self.password, self.ip, self.port,
                                                                       self.database)
        self.engine = create_engine(conInfo)
        print("数据库已经连接")
    pass

"""
    MySQL帮助类
"""
class MySqlHelper(sqlBaseHelper):
    def _getConnect_(self):
        print("正在尝试连接数据库")
        conInfo = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(self.userName, self.password, self.ip,self.port,self.database)
        self.engine = create_engine(conInfo)
        print("数据库已经连接")



"""
    sqlServer帮助类
"""
class sqlServerHelper(sqlBaseHelper):
    def _getConnect_(self):
        print("正在尝试连接数据库")
        conInfo = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(self.userName, self.password, self.ip, self.port,
                                                                       self.database)
        self.engine = create_engine(conInfo)
        print("数据库已经连接")



"""
    文件下载帮助类
    调用sql帮助类进行下载文件
"""
class downloadDataHelper():

    def __init__(self,ip, port, userName, password,database,downPath,type='mysql'):
        if type == 'mysql':
            self.sqlhelper = MySqlHelper(ip, port, userName, password, database)
        elif type =="oracle":
            self.sqlhelper = oracleHelper(ip, port, userName, password, database)
        elif type == "sqlserver":
            self.sqlhelper = sqlServerHelper(ip, port, userName, password, database)
        self.downPath = downPath

    def _saveTable2File_(self,table,columns=None,fileName=None):
        df = self.sqlhelper.getDataByTable(table,columns)
        if fileName == None:
            fileName = table
        self._tocsv_(df,fileName)

    # 将DataFrame存入到文件
    def _tocsv_(self,df,fileName):
        filePath = self.downPath+'\\'+fileName + '.csv'
        print("正在保存数据库表", filePath)
        df.to_csv(filePath, encoding='gbk')
        print("保存数据库表成功", filePath)

    def downloadTables(self,downConfig):
        for key in downConfig.keys():
            self._saveTable2File_(table=key,columns=downConfig.get(key))

    # 通过SQL进行下载数据
    def downloadDataBySQL(self,sqlStr,fileName):
        df = self.sqlhelper.getDataBySQL(sqlStr)
        self._tocsv_(df,fileName)

    # 通过SQL获取DataFrame结果集
    def getDataBySQL(self,sqlStr):
        return  self.sqlhelper.getDataBySQL(sqlStr)

