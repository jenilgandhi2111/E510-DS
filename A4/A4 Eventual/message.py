import ast
class Message():
    def __init__(self,messageType,sender,senderPort,senderHost,toPort,toHost,meta=None,broadCast = False):
        self.messageType = messageType
        self.sender = sender
        self.senderHost = senderHost
        self.senderPort = senderPort
        self.toHost = toHost
        self.toPort = toPort
        self.meta = meta
        self.broadcast = broadCast
        self.message = {"type":messageType,"sender":sender,"senderPort":senderPort,"senderHost":senderHost,"toPort":toPort,"toHost":toHost,"meta":meta,"broadcast":broadCast}
    
    def serializeMessage(self):
        # self.message = {"type":self.messageType,"sender":self.sender,"senderPort":self.senderPort,"senderHost":self.senderHost,"toPort":self.toPort,"toHost":self.toHost,"meta":self.meta,"broadcast":self.broadCast}
        return str(self.message)

    def getMessageType(self):
        return self.messageType


class SetMessage(Message):
    def __init__(self,key,value,sender,senderPort,senderHost,toPort,toHost,broadCast=True):
        data = {"key":key,"value":value}
        self.key = key
        self.value = value
        super().__init__("_SetMessage_",sender,senderPort,senderHost,toPort,toHost,data,broadCast)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        key = messageDict["meta"]["key"]
        value = messageDict["meta"]["value"]
        broadCast = bool(messageDict["broadcast"])
        return SetMessage(key,value,messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"],broadCast)
    
    def getRelayMessage(self,sender,toHost,toPort):
        key = self.key
        value = self.value
        return SetMessage(key,value,sender,self.toPort,self.toHost,toPort,toHost,False)
    
    def getAckMessage(self,sender,key):
        return SetAcknowledgementMessage(key,sender,self.toPort,self.toHost,self.senderPort,self.senderHost)

class GetMessage(Message):
    def __init__(self,key,sender,senderPort,senderHost,toPort,toHost,broadCast=True):
        data = {"key":key}
        self.key = key
        super().__init__("_GetMessage_",sender,senderPort,senderHost,toPort,toHost,data,broadCast)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        key = messageDict["meta"]["key"]

        return GetMessage(key,messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"],bool(messageDict["broadcast"]))


    def getAckMessage(self,sender,value):
        print(">>>>>>>",self.message,self.message["toPort"],self.message["senderPort"])
        return GetAcknowledgementMessage(value,sender,self.toPort,self.toHost,self.senderPort,self.senderHost)


class AcknowledgementMessage(Message):
    def __init__(self, messageType, sender, senderPort, senderHost, toPort, toHost, meta=None):
        super().__init__(messageType, sender, senderPort, senderHost, toPort, toHost, meta)
    

class GetAcknowledgementMessage(AcknowledgementMessage):
    def __init__(self,value, sender, senderPort, senderHost, toPort, toHost):
        self.value = value
        meta = {"value":value}
        super().__init__("_GetAckMessage_", sender, senderPort, senderHost, toPort, toHost, meta)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        value = messageDict["meta"]["value"]
        return GetAcknowledgementMessage(value,messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"])

class SetAcknowledgementMessage(AcknowledgementMessage):
    def __init__(self,key, sender, senderPort, senderHost, toPort, toHost):
        self.key = key
        meta = {"key":key}
        super().__init__("_SetAckMessage_", sender, senderPort, senderHost, toPort, toHost, meta)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        key = messageDict["meta"]["key"]
        return SetAcknowledgementMessage(key,messageDict["meta"]["sender"],messageDict["meta"]["senderPort"],messageDict["meta"]["senderHost"],messageDict["meta"]["toPort"],messageDict["meta"]["toHost"])
    