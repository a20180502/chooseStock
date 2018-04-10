import dataCollection


"""
获取数据，从数据库中获取数据并且保存到本地
"""
class downloadDataBase():

    def getDBConInfo(self):
        print('-----')
        return "connectInfo"

    def __init__(self):
        self.isConnect = False
        self.conInfo =self.getDBConInfo()


    def createCon(self):
        print("需要创建链接")
        return ""




    def getConInfo(self):

        return
    pass


class downloadData(downloadDataBase):

    def getDBConInfo(self):
        print('=====')
        return "sqlCon"

    def createCon(self):
        print("已经链接数据库"+self.conInfo)

    def getData(self):
        self.con = self.getConInfo()
        self.createCon()
        pass
    pass

d = downloadData()
d.getData()
print(d.isConnect)
