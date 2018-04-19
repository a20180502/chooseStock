"""
股票池获取,根据条件进行筛选股票池
"""
import pandas as pd


class downloadStockPool():
    def __init__(self,downloadDataHelper):
        self.ssh = downloadDataHelper

    def readstockpoollist(self, filepath):
        """

        :param filepath: 股票池配置文件保存路径
        :return:股票池信息列表(dataframe)
        """
        try:
            data = pd.read_csv(filepath,encoding='gbk')
            return data
        except IOError:
            print('read '+filepath+' error!')
            return None
        except FileNotFoundError:
            print('no stock pool find!')
            return None

    def readstockpool(self, filepath):
        """

        :param filepath: 股票池文件位置
        :return: 正在使用的股票池信息（np.narray）
        """
        data = self.readstockpoollist(filepath)
        if data is None:
            return None
        else:
            for indexs in data.index:
                if str(data.loc[indexs].values[-1]) == '1':
                    return data.loc[indexs].values[0:-1]
            return None

    def savestockpool(self, Id,Name,Market,Type,Industry,Plate,Hasst,Savepath,Isused,filepath):
        """

        :param Id: 编号，若是新增则为None或者0
        :param Name: 股票池名称
        :param Market: 市场
        :param Type: 股票类型
        :param Industry: 行业
        :param Plate: 板块
        :param Hasst: 是否去除ＳＴ，有ＳＴ１，没有０
        :param Savepath: 股票池详细信息保存路径
        :param Isused: 当前是否使用
        :param filepath: 股票池信息保存地址
        :return:
        """
        data = self.readstockpoollist(filepath)
        if data is None:
            return

        if Id is None or Id == 0:
            data.loc[data.shape[0]+1] = {'Id':data.shape[0]+1,'Name': Name,'Market': Market,'Type': Type,'Industry': Industry,'Plate': Plate,'Hasst': Hasst,'Savepath': Savepath,'Isused': Isused}
        else:
            data.loc[Id-1] = [Id,Name,Market,Type, Industry, Plate,Hasst, Savepath, Isused]

        try:
            data.to_csv(filepath, index=False)
        except IOError:
            print('save ' + filepath + ' error!')
            return
        except FileNotFoundError:
            print('no stock pool find!')
            return

    def downlaod_stocks(self, secumarket, secucategory, isst ,filename):
        """
        功能:根据条件进行下载股票池信息
            入参:
                secumarket:交易市场(83:SH,90:SZ)
                secucategory:股票类型(1:A股)
                isst: 是否包含ST(0:不包含,1:包含)
                filename: 股票池详细列表信息保存路径
        """
        if isst == 0:
            sqlstr = "select sm.InnerCode,sm.SecuCode,SecuAbbr,sm.SecuCategory,sm.SecuMarket,sm.CompanyCode,sm.ChiName,sm.ChiNameAbbr \
                     from SecuMain sm where sm.SecuCategory = {} and sm.SecuMarket = {} and  \
                     INSTR(sm.SecuAbbr,'st')<=0 "\
                     .format(secucategory, secumarket)
            #print(sqlstr)
        else:
            sqlstr = "select sm.InnerCode,sm.SecuCode,SecuAbbr,sm.SecuCategory,sm.SecuMarket,sm.CompanyCode,sm.ChiName,sm.ChiNameAbbr \
                      from SecuMain sm where sm.SecuCategory = %d and sm.SecuMarket = %d" \
                     % (secucategory, secumarket)
            #print(sqlstr)

        return self.ssh.downloadDataBySQL(sqlstr, filename)
