class Attribute:
    def __init__(self, name, type, length, is_null=True):
        self.name = name
        self.type = type
        self.length = length
        self.is_null = is_null

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type,
            "length": self.length,
            "is_null": self.is_null,
        }
