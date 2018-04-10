import pandas as pd
import helper.baseHelper as baseHelper
import os

"""
将下来过来的数据进行初次整理
主要功能：
    1：获取下载文件
    2：合并文件，将所有的文件保存到一个文件中
"""
class createTrainData():

    # 加载需要的配置信息
    def _loadConfig(self,configer):
        self._InnerCode_=configer.getConfig("DATA","InnerCode")
        self._CompanyCode_= configer.getConfig("DATA","CompanyCode")
        self._StockCode_= configer.getConfig("DATA","StockCode")
        self._delColumns_ = configer.getConfig("DATA","delColumns").split(',')


        self._path = configer.getConfig("FILE_PATH","downloadPath")
        self._trainFile_ = configer.getConfig("FILE_PATH","trainFile")
        self._initFile_ = configer.getConfig("FILE_PATH","initFile")


    # 获取路径下所有文件，过滤文件(初始化文件和训练文件)
    def _getAllFileName(self):
        fileList = baseHelper.filePathHelper.listFilesPath(self._path)
        if self._trainFile_ in fileList:
            fileList.remove(self._trainFile_)
        elif self._initFile_ in fileList:
            fileList.remove(self._initFile_)
        return  fileList

    # 进行文件的初始化（删除没必要的列）
    def _preprocessData_(self,df):
        # 删除文件中指定的列
        return df.drop(self._delColumns_,axis=1)


    def _loadData_(self):
        dataList = []
        for file in self._fileNameList:
            dataList.append(pd.read_csv(file))
        return dataList

    # 获取初始化文件并且校验文件是否存在必须的列
    def _getInitData(self):
        if os.path.exists(self._initFile_):
            self.initdf = pd.read_csv(self._initFile_)
            self.initdf = self._preprocessData_(self.initdf)
            columns = self.initdf.columns
            if self._InnerCode_ in columns and self._CompanyCode_ in columns and self._StockCode_ in columns:
                return True
            else:
                print("ERROR-初始化失败:初始化文件格式不对")
                return False
        else:
            print("ERROR-初始化失败:未找到初始化文件")
            return False



    def __init__(self):
        #self._logHelper = baseHelper.logHelper()
        self._loadConfig(baseHelper.configHelper())
        self._fileNameList = self._getAllFileName()
        self.initdf = pd.DataFrame()


    # 融合文件，将制定文件按照key进行融合
    def mergeData(self):
        # 获取初始化
        if not self._getInitData():
            print("ERROR-初始化失败")
            return False
        for file in self._fileNameList:
            dfTmp = pd.read_csv(file)
            dfTmp = self._preprocessData_(dfTmp)
            columns = dfTmp.columns
            if self._InnerCode_ in columns:
                self.initdf = pd.merge(self.initdf,dfTmp,how="outer",on=self._InnerCode_)
            elif self._CompanyCode_ in columns:
                self.initdf = pd.merge(self.initdf,dfTmp,how="outer",on=self._CompanyCode_)
            elif self._StockCode_ in columns:
                self.initdf = pd.merge(self.initdf,dfTmp,how="outer",on=self._StockCode_)
            else:
                print("无法合并文件:"+file)

        self.initdf.to_csv(self._trainFile_ )
        return True

# data = createTrainData()
# data.mergeData()