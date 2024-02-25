import hashlib


class Message:
    def __init__(self, blockId, lamportTime, message, totalBlocks):
        self.lamportTime = lamportTime
        self.message = message
        self.blockId = blockId
        self.hashValue = (
            str(self.blockId) + "," + str(self.lamportTime) + "," + str(self.message)
        )
        self.RemAcks = totalBlocks

    def computeHash(self):
        return str(
            hashlib.md5().update(
                (str(self.blockId) + str(self.lamportTime) + str(self.message)).encode()
            )
        )

    def serializeMessage(self):
        return (
            "MSG,"
            + str(self.blockId)
            + ","
            + str(self.lamportTime)
            + ","
            + str(self.message)
            + ","
            + str(self.hashValue)
        )

    def deserializeMessage(data: str, acks):
        data = data.split(",")
        message = Message(data[1], data[2], data[3], acks)
        return message

    def __str__(self):
        return (
            "MSG,"
            + str(self.blockId)
            + ","
            + str(self.lamportTime)
            + ","
            + str(self.message)
            + ",  HASH:"
            + str(self.hashValue)
        )


class AcknowledgeMessage:
    def __init__(self, hashValue):
        self.hashValue = hashValue

    def serializeMessage(self):
        return "ACK," + self.hashValue

    def deserializeMessage(data):
        if "ACK" in data:
            return AcknowledgeMessage(data[4:])
        return None
