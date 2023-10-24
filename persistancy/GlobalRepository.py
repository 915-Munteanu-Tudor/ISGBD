import os
import json


class GlobalRepository:
    """This class handles the management of the metadata of the dbmds from creation of databases, tables to indexes.
    @context_database : refers to the database in use on the server from calls like (USE DATABASE x)
    """

    def __init__(self):
        self.file_path = os.path.join(os.getcwd(), 'resources', 'catalog.json')
        # self.databases = self.readFromFile()
        self.databases = {}

    def readFromFile(self):
        if not os.path.exists(self.file_path):
            return {}

        with open(self.file_path, 'r') as f:
            return json.load(f)

    def writeToFile(self):
        # print(self.databases.get("test").to_dict())
        data_to_serialize = {name: table.to_dict() for name, table in self.databases.items()}
        with open(self.file_path, 'w') as f:
            json.dump(data_to_serialize, f, indent=4)
            # json.dump(self.databases.get("test").to_dict(), f, indent=4)

    def create_database(self, database):
        self.databases[database.name] = database
        self.writeToFile()

    def drop_database(self, database_name):
        self.databases.pop(database_name)
        self.writeToFile()

    def create_table(self, context_database, table):
        self.databases[context_database].tables[table.name] = table
        self.writeToFile()

    def drop_table(self, context_database, table_name):
        pass

    def create_index(self, context_database, index):
        pass
