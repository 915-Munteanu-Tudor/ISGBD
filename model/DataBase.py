class DataBase:
    def __init__(self, name):
        self.name = name
        self.tables = {} # key - name of table -> value : Table obj
        
    def to_dict(self):
        return {
            "name": self.name,
            "tables": [table.to_dict() for table in self.tables],
        }
