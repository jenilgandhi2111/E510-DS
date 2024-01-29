from Data import Data


class Client:
    def __init__(self, port):
        self.DataList = {}
        self.port = port

    def addData(self, key, data):
        dt = Data(data=data)
        if data not in self.DataList:
            self.DataList[key] = dt
            return
        self.DataList[key] = dt

    def getData(self, data):
        if data not in self.DataList:
            return ""
        return self.DataList[data].getData()
