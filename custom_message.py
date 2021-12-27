class DataNotFoundError(Exception):
    def __init__(self, data):
        self.message = f"{data} does not exist"

class WeighingItemConfirmed(Exception):
    pass

class PackingweightWeightQtyValidation(Exception):
    pass

class

try:
    if 2 > 1:
        raise DataNotFoundError
    a = 10

except Exception as e:
    raise Exception(e.message)