from dataCollection.downloadData import downloader
from helper.baseHelper import configHelper
from dataCollection.initTrainData import createTrainData
from featureEngineering.FeatureProcessing import cleanFeatures
from dataCollection.downloadLabel import downloadLabel

import pandas as pd
"""
    程序的主路线(中间件),进行该系统所有的流程控制
    按照流程进行调用类库,依次执行
"""
flow = [3,4,5]
""" 
    0 初始化,进行配置文件的加载
"""
configHelper = configHelper()
# 数据库连接串配置获取
IP = configHelper.getConfig("DataBaseInfo","IP")
DataBase = configHelper.getConfig("DataBaseInfo","DataBase")
UserName = configHelper.getConfig("DataBaseInfo","UserName")
Password = configHelper.getConfig("DataBaseInfo","Password")
Port = configHelper.getConfig("DataBaseInfo","Port")


# 文件路径配置加载
# 下载主路径(所有下载的文件存放的路径)
DownPath = configHelper.getConfig("FILE_PATH","downloadPath")
# 需要下载表名字段名的配置文件路径
downConfig = configHelper.getConfig("FILE_PATH","downConfig")
# 初始化文件路径
initFile = configHelper.getConfig("FILE_PATH", "initFile")
# 训练文件路径
trainFile = configHelper.getConfig("FILE_PATH", "trainFile")

# 文件合并配置加载
InnerCode = configHelper.getConfig("DATA", "InnerCode")
CompanyCode = configHelper.getConfig("DATA", "CompanyCode")
StockCode = configHelper.getConfig("DATA", "StockCode")
delColumns = configHelper.getConfig("DATA", "delColumns").split(',')

# 下载标签
timeBeg = configHelper.getConfig("DATAConfig", "timeBeg")
timeEnd = configHelper.getConfig("DATAConfig", "timeEnd")





""" 
    1 根据数据信息配置,进行数据的下载,并且保存到指定文件中
"""
if 1 in flow:
    # 加载配置文件,获取数据库连接信息\下载文件路径\表信息
    downloader = downloader(ip=IP, port=Port, userName=UserName, password=Password, database=DataBase, downPath=DownPath, type='mysql')
    downloader.down(downConfig)



""" 
    2 下载标签数据
    证券内码 filepath begindate
"""
stocksList=""
downloadLabel = downloadLabel(ip=IP, port=Port, userName=UserName, password=Password, database=DataBase, downPath=DownPath, type='mysql')
# downloadLabel.download_label(filepath=initFile,secucodes=stocksList,secucategory, begindate=timeBeg, enddate=timeEnd, timemod=, labelkinds=[[-999, -15, 'A'],[-15, 0, 'B'],[0, 15, 'C'],[15, 999, 'D']], secumarket=)

"""
    3 数据的合并,将下载过来的数据,合并成一个文件
"""
if 3 in flow:
    createTrainData = createTrainData()
    createTrainData.mergeData(mergerPath=DownPath,initFile=initFile,trainFile=trainFile,delColumns=['',''],keys=['',''])

"""
    4 进行数据清洗,主要是对异常值等特征进行去除
"""
if 4 in flow:
    df = pd.read_csv(trainFile)
    cleaner = cleanFeatures()
    cleaner.cleanData(df)



"""
    5 进行特征工程
"""

"""
    6 模型选择,进行模型的训练
"""



# 7 模型回测