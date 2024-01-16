class Index:
    def __init__(self, name, unique, attributes=None):
        if attributes is None:
            attributes = []
        self.name = (name if name.find('.ind') else "{}.ind".format(name))
        self.key_len = len(self.name)
        self.is_unique = unique
        self.type = "BTree"
        self.attributes = attributes

    def to_dict(self):
        return {
            "name": self.name,
            "keyLength": self.key_len,
            "is_unique": self.is_unique,
            "indexType": self.type,
            "attributes": [{"attribute_name": atr} for atr in self.attributes],
        }
