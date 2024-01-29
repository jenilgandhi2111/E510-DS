from Data import Data


class DataStore:
    def __init__(self):
        pass

    # Returns a data object
    def get(self, key) -> Data:
        # Fetch data from the file
        pass

    # Returns a string if the data is stored successfully
    # Returns "STORED\r\n" if set is successfull
    # Returns "NOT-STORED\r\n" if set is unsuccessfull
    def set(self, key, value) -> str:
        # Sets the data by making the object and storing it
        pass
