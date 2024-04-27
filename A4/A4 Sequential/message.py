# import ast
# class Message():
#     def __init__(self,messageType,sender,senderPort,senderHost,toPort,toHost,meta=None,broadCast = False):
#         self.messageType = messageType
#         self.sender = sender
#         self.senderHost = senderHost
#         self.senderPort = senderPort
#         self.toHost = toHost
#         self.toPort = toPort
#         self.meta = meta
#         self.broadcast = broadCast
#         self.hash = None
#         self.message =  None
#         self.constructMessage()
#     # def serializeMessage(self):
#     #     return str(self.message)

#     # def getMessageType(self):
#     #     return self.messageType
    
#     # def constructMessage(self):
#     #     self.hash = hash(str(self.meta))
#     #     self.message = {"type":self.messageType,"sender":self.sender,"senderPort":self.senderPort,"senderHost":self.senderHost,"toPort":self.toPort,"toHost":self.toHost,"meta":self.meta,"broadcast":self.broadCast}

#     # def getReplyMessage(message,sender,broadCast):
#     #     return Message(message.messageType,sender,message.toPort,message.toHost,message.senderPort,message.toHost,message.meta,broadCast)

    
    
import ast
import time
class Message():
    def __init__(self,messageType,sender,senderPort,senderHost,toPort,toHost,time,meta=None,broadcast = False,nacks = 0):
        self.messageType = messageType
        self.sender = sender
        self.senderPort = senderPort
        self.senderHost = senderHost
        self.toPort = toPort
        self.toHost = toHost
        self.meta = meta
        self.broadcast = broadcast
        self.nacks = nacks
        self.hash = None
        self.message = None
        self.timeStamp = time
        self.generateMessage()

    def __lt__(self, other):
        print(self,other)
        [c1, t1] = self.timeStamp.split(",")
        [c2, t2] = other.timeStamp.split(",")

        if t1==t2:
            return c1 < c2
        
        return t1 < t2


    def generateMessage(self):
        self.hash = hash(str(self.meta))
        self.message = {"type":self.messageType,"sender":self.sender,"senderPort":self.senderPort,"senderHost":self.senderHost,"toPort":self.toPort,"toHost":self.toHost,"meta":self.meta,"broadcast":self.broadcast,"nacks":self.nacks,"hash":self.hash,"time":self.timeStamp}

    def getReplyMessage(self,sender,broadcast = False,nacks=0):
        return Message(self.messageType,sender,self.toPort,self.toHost,self.senderPort,self.senderHost,self.meta,broadcast,nacks)
    
    def serializeMessage(self):
        return str(self.message)
    
    def getMessageType(self):
        return self.messageType
    

class SetMessage(Message):
    def __init__(self,key,value,sender,senderPort,senderHost,toPort,toHost,time,broadcast,nacks) -> None:
        self.key = key
        self.value = value
        data = {"key":key,"value":value}
        super().__init__("_SetMessage_",sender,senderPort,senderHost,toPort,toHost,time,data,broadcast,nacks)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        key = messageDict["meta"]["key"]
        value = messageDict["meta"]["value"]
        return SetMessage(key,value,messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"],messageDict["time"],messageDict["broadcast"],messageDict["nacks"])

    def getAckMessage(self,sender):
        return AcknowledgementMessage(sender,self.toPort,self.toHost,self.senderPort,self.senderHost,self.hash,"None")

    def replyMessage(self, sender):
        return SetReplyMessage(sender,self.toPort,self.toHost,self.senderPort,self.senderHost)


class GetMessage(Message):

    def __init__(self,key,sender,senderPort,senderHost,toPort,toHost,time,broadcast,nacks) -> None:
        data = {"key":key}
        self.key = key
        super().__init__("_GetMessage_",sender,senderPort,senderHost,toPort,toHost,time,data,broadcast,nacks)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        key = messageDict["meta"]["key"]
        print(messageDict)
        return GetMessage(key,messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"],messageDict["time"],bool(messageDict["broadcast"]),messageDict["nacks"])
    
    def getAckMessage(self,sender):
        return AcknowledgementMessage(sender,self.toPort,self.toHost,self.senderPort,self.senderHost,self.hash,"value")

    def replyMessage(self, sender,key,value):
        return GetReplyMessage(key,value,sender,self.toPort,self.toHost,self.senderPort,self.senderHost)


class SetReplyMessage(Message):
    def __init__(self, sender, senderPort, senderHost, toPort, toHost):
        super().__init__("_SetReplyMessage_", sender, senderPort, senderHost, toPort, toHost, {"reply":"<SET>"}, False, 0)

    def deserializeMsg(messageStr):
        return ast.literal_eval(messageStr)["meta"]["reply"]

class GetReplyMessage(Message):
    def __init__(self,key,value, sender, senderPort, senderHost, toPort, toHost):
        super().__init__("_GetReplyMessage_", sender, senderPort, senderHost, toPort, toHost, {"reply":f"KEY:{key},VALUE:{value}"}, False, 0)

    def deserializeMsg(messageStr):
        return ast.literal_eval(messageStr)["meta"]["reply"]
# class AcknowledgementMessage(Message):
#     def __init__(self, messageType, sender, senderPort, senderHost, toPort, toHost, meta=None):
#         super().__init__(messageType, sender, senderPort, senderHost, toPort, toHost, meta,False,0)

# class GetAcknowledgementMessage(AcknowledgementMessage):
#     def __init__(self, value, sender, senderPort, senderHost, toPort, toHost):
#         self.value = value
#         meta = {"value":value}
#         super().__init__("_GetAckMessage_", sender, senderPort, senderHost, toPort, toHost, meta)

#     def deserializeMessage(messageStr):
#         messageDict = ast.literal_eval(messageStr)
#         value = messageDict["meta"]["value"]
#         return GetAcknowledgementMessage(value,messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"])

    
# class SetAcknowledgementMessage(AcknowledgementMessage):
#     def __init__(self,key, sender, senderPort, senderHost, toPort, toHost):
#         self.key = key
#         meta = {"key":key}
#         super().__init__("_SetAckMessage_", sender, senderPort, senderHost, toPort, toHost, meta)
    
#     def deserializeMessage(messageStr):
#         messageDict = ast.literal_eval(messageStr)
#         key = messageDict["meta"]["key"]
#         return SetAcknowledgementMessage(key,messageDict["meta"]["sender"],messageDict["meta"]["senderPort"],messageDict["meta"]["senderHost"],messageDict["meta"]["toPort"],messageDict["meta"]["toHost"])
    
    

class AcknowledgementMessage(Message):
    def __init__(self,sender,senderPort,senderHost,toPort,toHost,hsh,other):
        self.messageHash = hsh
        meta = {"hash":hsh,"other":other}
        super().__init__("_ACK_",sender,senderPort,senderHost,toPort,toHost,0,meta,False,0)
    
    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return AcknowledgementMessage(messageDict["sender"],messageDict["senderPort"],messageDict["senderHost"],messageDict["toPort"],messageDict["toHost"],messageDict["meta"]["hash"],messageDict["meta"]["other"])
    
    def processAcknowledge(self,message:Message):
        if message.hash == self.messageHash:
            message.nacks -= 1
            return True
        return False
