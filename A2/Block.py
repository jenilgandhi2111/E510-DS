from Middleware import MiddleWare
from ApplicationLayer import ApplicationLayer
import json
import threading


class Block:
    def readConfig(self):
        with open(self.configFile, "r") as file:
            data = json.load(file)
            return data[str(self.blockId)]

    def spawnApplication(self):
        ApplicationLayer(
            self.blockId,
            self.config["Application"]["MiddlewareDown"]["port"],
            self.config["Application"]["MiddlewareDown"]["host"],
            self.config["Application"]["MiddlewareUp"]["port"],
            self.config["Application"]["MiddlewareUp"]["host"],
            self.testCase,
        )

    def spawnMiddleware(self):
        MiddleWare(
            self.blockId,
            self.config["Middleware"]["ApplicationUp"]["port"],
            self.config["Middleware"]["ApplicationUp"]["host"],
            self.config["Middleware"]["ApplicationDown"]["port"],
            self.config["Middleware"]["ApplicationDown"]["host"],
            self.config["Middleware"]["NetworkUp"]["port"],
            self.config["Middleware"]["NetworkUp"]["host"],
            self.config["Middleware"]["NetworkDown"]["port"],
            self.config["Middleware"]["NetworkDown"]["host"],
            self.testCase,
        )

    def __init__(self, blockId, testCaseId):
        self.blockId = blockId
        self.configFile = "./Config/" + testCaseId + "/" + testCaseId + ".json"
        self.config = self.readConfig()
        self.testCase = testCaseId

        threading.Thread(target=self.spawnMiddleware).start()
        threading.Thread(target=self.spawnApplication).start()
