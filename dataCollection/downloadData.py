import helper.downloadHelper


"""
获取数据，从数据库中获取数据并且保存到本地
"""


class downloadData():

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
