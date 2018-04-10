import pandas as pd
from helper.baseHelper import configHelper
"""
数据库链接帮助类
"""
configHelper = configHelper()
downPath = configHelper.getConfig("FILE_PATH","downloadPath")
IP = configHelper.getConfig("DataBaseInfo","IP")
DataBase = configHelper.getConfig("DataBaseInfo","DataBase")
UserName = configHelper.getConfig("DataBaseInfo","UserName")
Password = configHelper.getConfig("DataBaseInfo","Password")
Port = configHelper.getConfig("DataBaseInfo","Port")
downConfig = configHelper.getConfig("FILE_PATH","downConfig")

class oracleHelper():
    def connect(self):
        print("oracleHelper")
        pass
    pass




import pymysql
from sqlalchemy import create_engine
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

    def getDataByTable(self,table):
        print("正在读取数据库表", table)
        df = pd.read_sql_table(table, self.engine)
        print("读取数据库表完成", table)
        return df

    def getDataBySQL(self,sqlstr):
        df = pd.DataFrame()
        return df

    def saveTable2File(self,table,columns=None,fileName=None):
        df = self.getDataByTable(table)
        if fileName == None:
            fileName = table
        if columns == None:
            self._tocsv_(df,fileName)
        else:
            self._tocsv_(df[[columns]],fileName)


    # 将DataFrame存入到文件
    def _tocsv_(self,df,fileName):
        print("正在保存数据库表", fileName + '.csv')
        df.to_csv(fileName + '.csv', encoding='gbk')
        print("保存数据库表成功", fileName + '.csv')

    pass



class sqlSverHelper():
    def __init__(self):
        pass
    pass





class downloadData():
    def _getDownDataInfo_(self):
        self.dicConfig = {}
        tables = []
        dfDownConfig = pd.read_excel(downConfig)
        # if '表名' in dfDownConfig.columns and '字段' in dfDownConfig.columns:
        #     for table in dfDownConfig[['表名']]:
        #         self.dicConfig[table] = []
        #     for column in dfDownConfig[['字段']]:
        #         pass
        tables.append("")
        dfDownConfig[]

        pass

    def __init__(self,type):



        pass
    pass




mySqlHelper = MySqlHelper()
mySqlHelper.saveTable2File("SecuMain","")