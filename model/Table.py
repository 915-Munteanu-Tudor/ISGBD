class Table:
    def __init__(self, name):
        self.name = name
        self.attributes = []
        self.primary_key = []
        self.foreign_keys = {}
        self.unique_keys = []
        self.index_files = []

    def to_dict(self):
        return {
            "name": self.name,
            "primary_key": self.primary_key,
            "foreign_keys": self.foreign_keys,
            "attributes": [attr.to_dict() for attr in self.attributes],
            "index_files": [idx.to_dict() for idx in self.index_files],
        }
