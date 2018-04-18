from dataCollection.downloadData import downloadTables
from helper.baseHelper import configHelper
from dataCollection.initTrainData import createTrainData
from featureEngineering.FeatureProcessing import cleanFeatures
from dataCollection.downloadStockLabel import downloadStockLabel
from dataCollection.downloadStockPool import downloadStockPool
from helper.downloadHelper import downloadDataHelper
import pandas as pd
"""
    程序的主路线(中间件),进行该系统所有的流程控制
    按照流程进行调用类库,依次执行
"""
flow = []
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
# 股票池文件路径
stockPool = configHelper.getConfig("FILE_PATH", "stockPool")
# 训练文件路径
trainFile = configHelper.getConfig("FILE_PATH", "trainFile")

# 文件合并配置加载
InnerCode = configHelper.getConfig("DATA", "InnerCode")
CompanyCode = configHelper.getConfig("DATA", "CompanyCode")
StockCode = configHelper.getConfig("DATA", "StockCode")
delColumns = configHelper.getConfig("DATA", "delColumns").split(',')

# 下载标签
beginDate = configHelper.getConfig("DATAConfig", "timeBeg")
endDate = configHelper.getConfig("DATAConfig", "timeEnd")


# 声明下载器(所有连接数据库下载数据所需要的帮助类)
downloadDataHelper = downloadDataHelper(ip=IP, port=Port, userName=UserName, password=Password, database=DataBase, downPath=DownPath, type='mysql')


""" 
    1 根据配置信息,根据相应条件进行下载股票池信息
"""
downloadStockMessage = downloadStockPool(downloadDataHelper)
# downloadStockMessage.savestockpool(Id=1,Name='test1',Market='83',Type='1',Industry='',Plate='',Hasst='0',Savepath='SH_stocks.csv',Isused='1',filepath=r'C:\Users\zhangcf17306\Desktop\stockPool.csv')
#secumarket(83:SH,90:SZ) secucategory(1:A) isst(0:不含ST,1:包含)
downloadStockMessage.downlaod_stocks(secumarket='83', secucategory=1, isst=0 ,filename='SH_stocks')

""" 
    2 根据配置信息,根据条件将股票池中标记分类
"""
downloadLabel = downloadStockLabel(downloadDataHelper)
downloadLabel.download(stockpoolpath=r'C:\Users\zhangcf17306\Desktop\stockPool.csv', downloadfilepath=DownPath,labelkinds='', begindate=beginDate, enddate=endDate, labelfilename='SH_stocksLeable', timemod='quarter')

""" 
    3 根据数据(信息配置),进行数据的下载,并且保存到指定文件中
"""
if 1 in flow:
    # 加载配置文件,获取数据库连接信息\下载文件路径\表信息
    downloader = downloadTables(downloadDataHelper)
    downloader.download(downConfig)



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