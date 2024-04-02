# =========================================
# 	Author: Jenil J Gandhi
# 	Subject: Distributed Systems
# 	Assignment Number: 3
# 	Date: Date: 31-03-2024 16:48:29
# =========================================

import ast

"""
Description: Generic message template for sending messages.
"""


class Message:
    def __init__(self, messageType, sender, meta=None):
        self.sender = sender
        self.messageType = messageType
        self.message = {"messageType": messageType, "sender": sender, "meta": meta}

    def serializeMessage(self):
        return str(self.message)

    def getMessageType(self):
        return self.message["messageType"]


"""
Description: This is the message from mapper to master after the mapper is done with the mapping tasks.
"""


class DoneMessage(Message):
    def __init__(self, sender):
        super().__init__(messageType="_Done_", sender=sender)

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return DoneMessage(messageDict["sender"])


"""
Description: This the message that the master node will send when it has recived all done messages from all
mappers. This would include port and host info of each reducer.
"""


class SendToReducerMessage(Message):
    def __init__(self, sender, reducerPortInfo):
        super().__init__(
            messageType="_SendToReducer_", sender=sender, meta=reducerPortInfo
        )
        self.reducerPorts = []
        self.reducerHosts = []
        # Maps data from reducerportInfo
        for reducerData in reducerPortInfo:
            self.reducerPorts.append(reducerData["recvPort"])
            self.reducerHosts.append(reducerData["host"])

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return SendToReducerMessage(messageDict["sender"], messageDict["meta"])


"""
Description: Message that the mapper would send to the reducer while sending data will have key and value field
also, a sender field to identify the mapper who sent it.
"""


class ReducerDataMessage(Message):
    def __init__(self, sender, key, value):
        super().__init__("_ReducerDataMessage_", sender, {key: value})
        self.key = key
        self.value = value

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        # return ReducerDataMessage(messageDict["sender"], "A", "B")
        key = list(messageDict["meta"].keys())[0]
        value = messageDict["meta"][key]
        return ReducerDataMessage(
            messageDict["sender"],
            key,
            value,
        )


"""
Description: Message that the mapper or the reducer would send as a pulse to mark that the mapper or the reducer is alive.
the time that the messages would be sent is 1second and the timeout on the master is 2second
"""


class PulseMessage(Message):
    def __init__(self, sender):
        super().__init__("_PulseMessage_", sender)

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return PulseMessage(messageDict["sender"])


"""
Description: Kill message is the SIGINT kind of message which would kill the process
"""


class KillMessage(Message):
    def __init__(self, sender, meta=None):
        super().__init__("_KillMessage_", sender, meta)

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return KillMessage(messageDict["sender"])


"""
Description: Clear message would signal clearing input buffer to the reducers for the fault tolerance
"""


class ClearMessage(Message):
    def __init__(self, sender, meta=None):
        super().__init__("_ClearMessage_", sender, meta)

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return ClearMessage(messageDict["sender"])


"""
Description: Stop message signals that there has been a fault and each mapper if they are sending data should
stop it right away.
"""


class StopMessage(Message):
    def __init__(self, sender, meta=None):
        super().__init__("_StopMessage_", sender, meta)

    def deserializeMessage(messageStr):
        messageDict = ast.literal_eval(messageStr)
        return ClearMessage(messageDict["sender"])
