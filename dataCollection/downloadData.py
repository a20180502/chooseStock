from helper.downloadHelper import downloadDataHelper
import pandas as pd

"""
    下载器功能:连接数据库,根据配置文件从数据库获取数据,并且保存到本地
"""
class downloader():

    # 初始化下载器
    # 入参:数据库连接信息
    def __init__(self,ip, port, userName, password,database,downPath,type):
        self.downloadHelper = downloadDataHelper(ip, port, userName, password, database, downPath, type)

    # 从配置文件中,获取下载的数据信息,
    # 入参:
    #   配置文件路径
    # 出参:
    #   返回配置文件中的信息,以字典形式返回:{table1:[columns],table1:[columns]}
    def getDownDataInfo(self, downConfig):
        setTables = set()
        tmpDic = {}
        dfDownConfig = pd.read_excel(downConfig)
        if ('表名' in dfDownConfig.columns) and ('字段' in dfDownConfig.columns):
            print(dfDownConfig['表名'].tolist())
            for table in dfDownConfig['表名'].tolist():
                setTables.add(table.strip())
            for table in setTables:
                tmpDic[table] = dfDownConfig[dfDownConfig['表名'] == table]['字段'].tolist()
        else:
            print('文件格式不合法')
        return tmpDic

    # 下载文件,根据配置文件,进行下载指定表和字段
    # 入参:配置文件路径
    def down(self,downConfig):
        downConfig = self.getDownDataInfo(downConfig)
        self.downloadHelper.downloadTables(downConfig)
