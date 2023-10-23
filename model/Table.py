class Table:
    def __init__(self, name):
        self.name = name
        self.file_name = name + ".bin"
        self.attributes = []
        self.primary_key = []
        self.foreign_keys = {}
        self.unique_keys = []
        self.index_files = []

    def to_dict(self):
        return {
            "name": self.name,
            "primary_key": self.primary_key,
            "attributes": [attr.to_dict() for attr in self.attributes],
        }
