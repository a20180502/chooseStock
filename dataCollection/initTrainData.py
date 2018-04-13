import pandas as pd
import helper.baseHelper as baseHelper
import os

"""
将下载的数据进行初次整理
主要功能：
    1：获取下载文件
    2：合并文件，将所有的文件保存到一个文件中
"""
class createTrainData():

    # 获取路径下所有文件，过滤文件(初始化文件和训练文件)
    def _getAllFileName(self,mergerPath):
        fileList = baseHelper.filePathHelper.listFilesPath(mergerPath)
        return  fileList




    # 获取初始化文件并且校验文件是否存在必须的列
    def _getInitData(self,initFile,keys,delColumns):
        if os.path.exists(initFile):
            initdf = pd.read_csv(initFile).drop(delColumns,axis=1)
            columns = initdf.columns
            if keys in columns:
                return initdf
            else:
                print("ERROR-初始化失败:初始化文件格式不对")
                return None
        else:
            print("ERROR-初始化失败:未找到初始化文件")
            return None

    # 融合文件，将文件按照key进行合并
    def mergeData(self,mergerPath,initFile,trainFile,delColumns,keys):
        initdf = self._getInitData(initFile,keys,delColumns)
        fileNameList = self._getAllFileName(mergerPath)
        # 获取初始化
        if  initdf == None:
            print("ERROR-初始化失败")
        else:
            for file in fileNameList:
                dfTmp = pd.read_csv(file)
                columns = dfTmp.columns
                flag = False
                for key in keys:
                    if key in columns:
                        flag = True
                        initdf = pd.merge(initdf, dfTmp, how="outer", on=key)
                        break
                if not flag:
                    print("无法合并文件:"+file,'不存在主键:',keys)
            initdf.to_csv(trainFile)

# path = configer.getConfig("FILE_PATH","downloadPath")
# data = createTrainData()
# data.mergeData()