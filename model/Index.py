class Index:
    def __init__(self, name):
        self.name = "{}.ind".format(name)
        self.key_len = len(self.name)
        self.is_unique = 1
        self.type = "BTree"
        self.attributes = []

    def to_dict(self):
        return {
            "name": self.name,
            "keyLength": self.key_len,
            "is_unique": self.is_unique,
            "indexType": self.type,
            "attributes": [{"attribute_name": atr} for atr in self.attributes],
        }
