import pandas as pd
import helper.baseHelper as helper

"""
A:特征清洗，主要功能是去除异常样本
# 1. 根据业务去掉不需要的列
# 2. 去除所有行以及所有的列均为nan的数据
# 3. 去除缺失率高的特征,默认超过0.99则不考虑
# 4. 去除特征中包含实例类别太多的特征，默认包含1000个实例则不考虑
# 5. 对于数值型，如果标准差太小的，则不考虑该特征
# 6. 样本在重要特征缺失情况如果比较多，则把这些行去掉
# 7. 文本字符的处理（对于标称值，进行归类，比如大小写归并，同词义的不同的词汇归并，将有空格的去掉等；）
# 8. 按业务逻辑完全不可解释的变量直接剔除
# 5. 对缺失值的多维度处理
# 6. 对离散点的处理
"""
class cleanFeatures():

    def __init__(self, configer=None, loger=None):
        self.configer = configer
        self.loger = loger



    # 记录数据集的相关信息
    def recordDataInfo(self, msg, level='4'):
        msg = "数据清洗cleanData:"+msg
        if not self.loger == None :
            self.loger.logMsg(level,msg)
        else:
            print(msg)

    # 取出X,y
    def getDataX_Y(self, df, targetCol):
        X = df.loc[:, df.columns != targetCol]
        y = df[targetCol]
        X.is_copy = False
        y.is_copy = False
        return X, y

    # 1 删除列
    def dropByColumn(self, df_X, delColumns=None):
        self.recordDataInfo("开始进行[删除指定列]操作...")
        if delColumns == None:
            self.recordDataInfo("没有指定删除列,此过程[删除指定列]跳过！")
        else:
            self.recordDataInfo("开始删除指定列：delColumns{}".format(delColumns))
            self.recordDataInfo("删除列操作之前：shape{}".format(df_X.shape))
            df_X.drop(delColumns, axis=1, inplace=True)
            self.recordDataInfo("删除列操作之后：shape{}".format(df_X.shape))

    # 2 去除所有行以及所有的列均为nan的数据
    def dropNan(self, df_X):
        self.recordDataInfo("开始进行[删除空行空列]操作...")
        self.recordDataInfo("[删除空行空列]操作之前：shape{}".format(df_X.shape))
        # 去除所有行为NaN
        df_X.dropna(axis=0, how="all", inplace=True)
        self.recordDataInfo("[删除空行]操作之后：shape{}".format(df_X.shape))
        # 去除所有列为NaN的
        df_X.dropna(axis=1, how='all', inplace=True)
        self.recordDataInfo("[删除空列]操作之后：shape{}".format(df_X.shape))

    # 3 去除缺失率高的特征,默认超过0.99则不考虑
    def drop_missing_features(self, df, missing_thread_hold=0.99):
        self.recordDataInfo("开始进行[去除缺失率高的特征]操作,去除缺失率大于"+str(missing_thread_hold*100)+"%")
        self.recordDataInfo("[去除缺失率高的特征]操作之前：shape{}".format(df.shape))
        # 定义嵌套函数
        def count_missing_features(type):
            df1 = df.select_dtypes(include=[type])
            # df1.columns 不允许为0
            if df1.columns.size != 0:
                df1 = df1.describe().T
                df2 = df1.assign(missing_pct=df.apply(lambda x: (len(x) - x.count()) / float(len(x))))
                # 取出这些特征
                df3 = df2[df2['missing_pct'] >= missing_thread_hold]
                return list(df3.index)
            # else分支处理，返回的类型要跟接收的一样
            return []

        # 函数调用
        missing_features = []
        missing_features.extend(count_missing_features('number'))
        missing_features.extend(count_missing_features('object'))
        df.drop(missing_features, axis=1, inplace=True)
        self.recordDataInfo("[去除缺失率高的特征]操作之后：shape{}".format(df.shape))

    # 4 去除特征中包含实例类别太多的特征，默认包含1000个实例则不考虑
    def drop_over_instance_features(self, df, over_instance_thread_hold=1000):
        self.recordDataInfo("开始进行[去除实例多的特征]操作,去除特征实例大于"+str(over_instance_thread_hold))
        self.recordDataInfo("[去除实例多的特征]操作之前：shape{}".format(df.shape))
        features = []
        # 获取string类型的特征
        cols = df.select_dtypes(include=['object']).columns
        for col in cols:
            num = len(df[col].unique())
            if num >= over_instance_thread_hold:
                self.recordDataInfo("[去除实例多的特征]操作之前：shape{}".format(df.shape))
                self.recordDataInfo("[去除实例多的特征]列'{}'的实例有{},去除".format(col,len(df[col].unique())))
                features.append(col)
        if len(features) != 0:
            df.drop(features, axis=1, inplace=True)
        self.recordDataInfo("[去除实例多的特征]操作之后：shape{}".format(df.shape))


    # 5 去除标准差小的特征
    def dropFeaturesByStd(self, df, std_thread_hold=0.1):
        self.recordDataInfo("开始进行[去除小标准差特征]操作,去除标准差小与"+str(std_thread_hold))
        self.recordDataInfo("[去除小标准差特征]操作之前：shape{}".format(df.shape))
        # 只针对float类型
        df_float = df.select_dtypes(include=["float"])
        if df_float is not None:
            features = df_float.columns[df_float.std() <= std_thread_hold].tolist()
            self.recordDataInfo("[去除小标准差特征]有{}个特征小与{}".format(len(features),std_thread_hold))
            df.drop(features, axis=1, inplace=True)
        self.recordDataInfo("[去除小标准差特征]操作之后：shape{}".format(df.shape))

    # 6 删除样本在重要特征缺失情况如果比较多的
    def dropMissing_important_feature(self, df, features, miss_pct_thread_hold=0.5):
        self.recordDataInfo("开始进行[去除缺失率高的重要特征]操作,缺失率大与"+str(miss_pct_thread_hold))
        self.recordDataInfo("[去除缺失率高的重要特征]操作之前：shape{}".format(df.shape))
        # 内部函数，计算缺失率
        def _num_missing(df):
            return sum(df.isnull()) / float(len(df))

        ss1 = df.loc[:, features].apply(_num_missing, axis=1)
        # 获取缺失值超过阈值的index
        drop_idx = ss1[ss1 >= miss_pct_thread_hold].index
        # 根据index，删除指定的行
        df.drop(drop_idx, axis=0, inplace=True)
        self.recordDataInfo("[去除缺失率高的重要特征]操作之后：shape{}".format(df.shape))



    def cleanData(self,df):

        self.dropByColumn(df)
        self.dropNan(df)
        self.drop_missing_features(df)
        self.drop_over_instance_features(df)
        self.dropFeaturesByStd(df)
        self.dropMissing_important_feature(df,["area"])

# 使用案例
# df = pd.read_csv("SHAstock_basics.csv",encoding='GBK')
# cd = cleanData(helper.configHelper(),helper.logHelper())
# df = cd.getDataX_Y(df, "code")[0]
# cd.cleanData(df)
# print(df.shape)
# print(df)

"""
B:数据采样
# 1：样本不均衡采样
    通过过抽样和欠抽样解决样本不均衡
    通过正负样本的惩罚权重解决样本不均衡
    通过组合/集成方法解决样本不均衡
    通过特征选择解决样本不均衡
# 2：样本权重
"""
from imblearn.combine import SMOTEENN
class sampleData():

    def smote(self):
        pass

    pass


from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
"""
C:处理单个特征：
#1：归一化
#2：离散化
#3：哑编码
#4：缺失值
#5：数据变换
"""
class singleFuture():

    # 标准化
    def standardScaler(self,df_X):
        StandardScaler().fit_transform(df_X)

    # 归一化
    def normalization(self,df_X):
        MinMaxScaler().fit_transform(df_X)
        pass



    pass


from sklearn.decomposition import PCA
"""
D:处理多个特征：
    1：降维PCA、LDA
    2：特征选择

"""
class mulfeature():
    def pca(self):
        PCA.fit_transform()
        pass

    def lda(self):
        pass

    pass




"""
衍生变量：
"""
class derivedFeature():
    pass