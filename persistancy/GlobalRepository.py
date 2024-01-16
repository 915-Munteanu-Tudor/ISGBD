import os
import json
from model.Attribute import Attribute
from model.DataBase import DataBase
from model.Index import Index
from model.Table import Table
from pymongo import MongoClient
from pymongo.server_api import ServerApi


class GlobalRepository:
    """This class handles the management of the metadata of the dbmds from creation of databases, tables to indexes.
    @context_database : refers to the database in use on the server from calls like (USE DATABASE x)
    """

    def __init__(self):
        self.mongo_client = MongoClient(
            "mongodb+srv://tdr:JbEMLiFXtrnlt8LT@cluster0.lycgfjj.mongodb.net/?retryWrites=true&w=majority",
            server_api=ServerApi('1'))
        self.file_path = os.path.join(os.getcwd(), 'resources', 'catalog.json')
        self.databases = self.read_from_file()
        self.connect_to_mongo()
        # self.databases = {}

    def connect_to_mongo(self):
        try:
            self.mongo_client.admin.command('ping')
            print("Successfully connected to MongoDB!")
        except Exception as e:
            print(e)

    def read_from_file(self):
        if not os.path.exists(self.file_path):
            return {}

        with open(self.file_path, 'r') as f:
            data = json.load(f)
            databases = {}
            for db_name, db_data in data.items():
                database = DataBase(db_name)
                for table_data in db_data["tables"]:
                    table = Table(table_data["name"])
                    for attr_data in table_data["attributes"]:
                        attribute = Attribute(attr_data["name"], attr_data["type"], attr_data["length"], attr_data["is_null"])
                        table.attributes.append(attribute)
                    for key in table_data.get("primary_key", []):
                        table.primary_key.append(key)
                    for key, value in table_data.get("foreign_keys", {}).items():
                        table.foreign_keys[key] = value
                    for idx_data in table_data["index_files"]:
                        index = Index(idx_data["name"], idx_data["is_unique"])
                        for attr in idx_data["attributes"]:
                            index.attributes.append(attr["attribute_name"])
                        table.index_files.append(index)
                    database.tables[table_data["name"]] = table
                databases[db_name] = database
            return databases

    def write_to_file(self):
        data_to_serialize = {name: table.to_dict() for name, table in self.databases.items()}
        with open(self.file_path, 'w') as f:
            json.dump(data_to_serialize, f, indent=4)

    def create_database(self, database):
        self.databases[database.name] = database
        self.write_to_file()

    def drop_database(self, database_name):
        self.databases.pop(database_name)
        self.write_to_file()

    def create_table(self, context_database, table):
        self.databases[context_database].tables[table.name] = table
        self.write_to_file()

    def drop_table(self, context_database, table_name):
        self.databases[context_database].tables.pop(table_name)
        self.write_to_file()

    def create_index(self, context_database, table_name, index):
        self.databases[context_database].tables[table_name].index_files.append(index)
        self.write_to_file()
