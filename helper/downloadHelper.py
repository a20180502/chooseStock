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
downPath = configHelper.getConfig("FILE_PATH","downloadPath")
IP = configHelper.getConfig("DataBaseInfo","IP")
DataBase = configHelper.getConfig("DataBaseInfo","DataBase")
UserName = configHelper.getConfig("DataBaseInfo","UserName")
Password = configHelper.getConfig("DataBaseInfo","Password")
Port = configHelper.getConfig("DataBaseInfo","Port")
downConfig = configHelper.getConfig("FILE_PATH","downConfig")


"""
    Oracle帮助类
"""
class oracleHelper():
    def connect(self):
        print("oracleHelper")
        pass

    def getDataByTable(self, table, columns):
        print("正在读取数据库表", table)
        df = pd.read_sql_table(table, self.engine, columns=columns)
        print("读取数据库表完成", table)
        return df
    pass

"""
    MySQL帮助类
"""
class MySqlHelper():
    def __init__(self,ip=IP,port = Port, userName=UserName,password=Password,database=DataBase):
        # self.connectInfo = pymysql.connect(ip,userName,password,database)
        # self.cursor = self.connectInfo.cursor()
        self.ip = ip
        self.port = port
        self.userName = userName
        self.password = password
        self.database = database
        self._getConnect_()

    def _getConnect_(self):
        print("正在尝试连接数据库")
        conInfo = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(self.userName, self.password, self.ip,self.port,self.database)
        self.engine = create_engine(conInfo)
        print("数据库已经连接")

    # 根据表名\字段名\日期范围\证券代码从数据库中获取数据
    def getDataByTable(self,table,columns=None,dateRange=None,stockList=None):
        print("正在读取数据库表", table,'字段',columns)
        df = pd.read_sql_table(table,self.engine,columns=columns)
        print("读取数据库表完成", table)
        return df

    # 根据SQL从数据库中获取数据
    def getDataBySQL(self,sqlstr):
        print("正在执行SQL,读取数据:", sqlstr)
        df = pd.read_sql(sqlstr,self.engine)
        print("执行SQL完毕,读取数据描述:")
        print(df.describe())
        return df

"""
    sqlServer帮助类
"""
class sqlServerHelper():
    def __init__(self):
        pass

    def getDataByTable(self, table, columns):
        print("正在读取数据库表", table)
        df = pd.read_sql_table(table, self.engine, columns=columns)
        print("读取数据库表完成", table)
        return df
    pass

"""
    文件下载帮助类
    调用sql帮助类进行下载文件
"""
class downloadDataHelper():

    def __init__(self,type='mysql'):
        if type == 'mysql':
            self.sqlhelper = MySqlHelper()
        elif type =="oracle":
            self.sqlhelper = oracleHelper()
        elif type == "sqlserver":
            self.sqlhelper = sqlServerHelper()

    def _getDownDataInfo_(self,downConfig=downConfig):
        setTables = set()
        tmpDic = {}
        dfDownConfig = pd.read_excel(downConfig)
        if ('表名' in dfDownConfig.columns) and ('字段' in dfDownConfig.columns):
            print(dfDownConfig['表名'].tolist())
            for table in dfDownConfig['表名'].tolist():
                setTables.add(table.strip())
            for table in setTables:
                tmpDic[table]=dfDownConfig[dfDownConfig['表名']== table]['字段'].tolist()
        else:
            print('文件格式不合法')
        return tmpDic

    def _saveTable2File_(self,table,columns=None,fileName=None):
        df = self.sqlhelper.getDataByTable(table,columns)
        if fileName == None:
            fileName = table
        self._tocsv_(df,fileName)

    # 将DataFrame存入到文件
    def _tocsv_(self,df,fileName):
        filePath = downPath+'\\'+fileName + '.csv'
        print("正在保存数据库表", filePath)
        df.to_csv(filePath, encoding='gbk')
        print("保存数据库表成功", filePath)

    def downloadTables(self):
        downConfig = self._getDownDataInfo_()
        for key in downConfig.keys():
            self._saveTable2File_(table=key,columns=downConfig.get(key))

    # 通过SQL进行下载数据
    def downloadDataBySQL(self,sqlStr,fileName):
        df = self.sqlhelper.getDataBySQL(sqlStr)
        self._tocsv_(df,fileName)



#
# mySqlHelper = MySqlHelper()
# mySqlHelper.saveTable2File("SecuMain")
helper = downloadDataHelper()
helper.downloadDataBySQL("select InnerCode,CompanyCode,SecuCode from SecuMain","SecuMainSQL")