import re

from pymongo.errors import DuplicateKeyError

from model.Attribute import Attribute
from model.Index import Index
from model.Table import Table
from model.DataBase import DataBase
from persistancy.GlobalRepository import GlobalRepository


class SqlParser:
    def __init__(self):
        self.global_repo = GlobalRepository()
        self.used_db = None

    @staticmethod
    def cleanup_command(command, split=True):
        """removes whitespaces and capitalizes the string for easier processing"""
        command = re.sub(" +", " ", command).strip().upper()

        if split:
            command = command.split()

        return command

    def parse_create_table(self, sql):
        """Parses a sql create table command and returns the new Table object
        @return: Table
        """

        if self.used_db is None:
            return "Please use a database first."

        sql = SqlParser.cleanup_command(sql, False)
        words = sql.split()

        table_name = words[2]

        if table_name in self.global_repo.databases[self.used_db].tables.keys():
            return "The used database already contains a table with this name."

        table = Table(table_name)

        attr_defs = sql[sql.index("(") + 1: sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",")]

        for attr in attr_list:
            length = None
            is_null = True

            if "NOT NULL" in attr:
                is_null = False
                attr = attr.replace("NOT NULL", "").strip()

            # Check for PRIMARY KEY or REFERENCES (foreign key)
            if attr.strip().startswith("PRIMARY KEY"):
                keys = attr.split("(")[1].split(")")[0].split(",")
                keys = [key.strip() for key in keys]
                table.primary_key.extend(keys)
                continue
            elif attr.strip().startswith("UNIQUE"):
                keys = attr.split("(")[1].split(")")[0].split(",")
                keys = [key.strip() for key in keys]
                table.index_files.append(Index(f"{table_name}_{'-'.join(keys)}", 1, keys))
                continue
            else:
                if "PRIMARY KEY" in attr:
                    key_info = re.findall(r"\w+", attr.split("PRIMARY KEY")[0])
                    key_name = key_info[0]
                    key_type = key_info[1]
                    key_length = key_info[2] if len(key_info) > 2 else None
                    new_attribute = Attribute(
                        name=key_name, type=key_type, length=key_length, is_null=is_null
                    )
                    table.attributes.append(new_attribute)
                    table.primary_key.append(key_name)
                    table.index_files.append(Index(f"{table_name}_{key_name}", 0, [key_name]))
                    continue
                elif "REFERENCES" in attr:
                    attr_info = re.findall(r"\w+", attr.split("REFERENCES")[0])
                    attr_name = attr_info[0]
                    attr_type = attr_info[1]
                    attr_length = attr_info[2] if len(attr_info) > 2 else None
                    foreign_key = re.findall(r"\w+", attr.split("REFERENCES")[1])
                    if foreign_key[0] not in self.global_repo.databases[self.used_db].tables.keys():
                        return "Cannot reference a nonexistent table."
                    existent_columns = [col.name for col in
                                        self.global_repo.databases[self.used_db].tables[foreign_key[0]].attributes]
                    if foreign_key[1] not in existent_columns:
                        return "Cannot reference a nonexistent column."
                    foreign_key = {"table": foreign_key[0], "attribute": foreign_key[1]}
                    table.foreign_keys[attr_name] = foreign_key
                    new_attribute = Attribute(
                        name=attr_name, type=attr_type, length=attr_length, is_null=is_null
                    )
                    table.attributes.append(new_attribute)
                    table.index_files.append(Index(f"{table_name}_{new_attribute.name}",
                                                   0, [new_attribute.name]))
                    continue

            # Extract basic attribute details (name, type, length)
            attr_info = re.findall(r"\w+", attr)
            name = attr_info[0]
            type = attr_info[1]
            if len(attr_info) > 2:
                length = int(attr_info[2])

            # Create and add the Attribute object
            attribute = Attribute(name, type, length, is_null)
            table.attributes.append(attribute)

        self.global_repo.create_table(self.used_db, table)
        return "Table {} created successfully.".format(table_name)

    def parse_create_database(self, sql):
        if len(sql) != 3:
            return "Incorrect syntax."

        db_name = sql[2]
        if db_name in self.global_repo.databases.keys():
            return "The database name is taken."

        self.global_repo.create_database(DataBase(db_name))
        return "Database {} created successfully.".format(db_name)

    def parse_drop_database(self, sql):
        if len(sql) != 3:
            return "Incorrect syntax."

        db_name = sql[2]
        if db_name in self.global_repo.databases.keys():
            self.global_repo.drop_database(db_name)
            if self.used_db == db_name:
                self.used_db = None
            return "Database {} dropped successfully.".format(db_name)

        return "The database you want to drop does not exist."

    def parse_use_database(self, sql):
        if len(sql) != 3:
            return "Incorrect syntax."

        db_name = sql[2]
        if db_name in self.global_repo.databases.keys():
            self.used_db = db_name
            return "The used database is {}.".format(self.used_db)

        return "The database you want to use does not exist."

    def parse_drop_table(self, sql):
        if len(sql) != 3:
            return "Incorrect syntax."

        existing = False
        table_name = sql[2]

        for tbl in self.global_repo.databases[self.used_db].tables.values():
            if tbl.name != table_name:
                for fk in tbl.foreign_keys.values():
                    references = fk['table']
                    if references == table_name:
                        return ('Cannot drop table "{}" because child table "{}" references it.'
                                .format(table_name, tbl.name))
            else:
                existing = True

        if existing:
            self.global_repo.drop_table(self.used_db, table_name)
            return "Table {} dropped successfully.".format(table_name)

        return "The table you want to drop does not exist."

    def parse_create_index(self, sql):
        if self.used_db is None:
            return "Please use a database first."

        sql = SqlParser.cleanup_command(sql, False)
        words = sql.split()

        table_name = words[4]
        index_name = words[2]

        if table_name not in self.global_repo.databases[self.used_db].tables.keys():
            return "The table you want to create index on, does not exist."

        indexes = self.global_repo.databases[self.used_db].tables[table_name].index_files
        if len(indexes) > 0:
            for idx in indexes:
                if index_name == idx.name.split('.')[0]:
                    return "There is already an index with this name on the table."

        index = Index(index_name, 0)

        attr_defs = sql[sql.index("(") + 1: sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",")]

        table_columns = [col.name for col in self.global_repo.databases[self.used_db].tables[table_name].attributes]
        nonexistent_columns = [col for col in attr_list if col not in table_columns]
        if len(nonexistent_columns) > 0:
            return "Columns " + ", ".join(nonexistent_columns) + " don't exist."

        for attr in attr_list:
            index.attributes.append(attr)

        self.global_repo.create_index(self.used_db, table_name, index)
        return "The index {} on table {} was crated".format(index_name, table_name)

    def parse_create_unique_index(self, sql):
        if self.used_db is None:
            return "Please use a database first."

        sql = SqlParser.cleanup_command(sql, False)
        words = sql.split()

        table_name = words[5]
        index_name = words[3]

        if table_name not in self.global_repo.databases[self.used_db].tables.keys():
            return "The table you want to create index on, does not exist."

        indexes = self.global_repo.databases[self.used_db].tables[table_name].index_files
        if len(indexes) > 0:
            for idx in indexes:
                if index_name == idx.name.split('.')[0]:
                    return "There is already an index with this name on the table."

        index = Index(index_name, 1)

        attr_defs = sql[sql.index("(") + 1: sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",")]

        table_columns = [col.name for col in self.global_repo.databases[self.used_db].tables[table_name].attributes]
        nonexistent_columns = [col for col in attr_list if col not in table_columns]
        if len(nonexistent_columns) > 0:
            return "Columns " + ", ".join(nonexistent_columns) + " don't exist."

        for attr in attr_list:
            index.attributes.append(attr)

        self.global_repo.create_index(self.used_db, table_name, index)
        return "The index {} on table {} was crated".format(index_name, table_name)

    def parse_insert(self, sql):
        if self.used_db is None:
            return "Please use a database first."

        sql = SqlParser.cleanup_command(sql, False)
        words = sql.split()

        table_name = words[2]

        if table_name not in self.global_repo.databases[self.used_db].tables.keys():
            return "The table you want to insert into does not exist."

        attr_defs = sql[sql.index("(") + 1: sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",")]

        db = self.global_repo.mongo_client[self.used_db]
        collection = db[table_name]

        table = self.global_repo.databases[self.used_db].tables.get(table_name)
        actual_table_columns = [col for col in table.attributes]
        actual_primary_key = table.primary_key

        column_names = [col.name for col in actual_table_columns]

        if len(column_names) != len(attr_list):
            return "Please insert values for each column."

        primary_key_pos = column_names.index(actual_primary_key[0])
        primary_key = attr_list[primary_key_pos]

        for i in range(0, len(attr_list)):
            if attr_list[i] == "NULL" and actual_table_columns[i].is_null is False:
                return "{} cannot be null".format(actual_table_columns[i].name)

            if actual_table_columns[i].type == "INT":
                try:
                    int(attr_list[i])
                except ValueError:
                    return "{} has int type.".format(actual_table_columns[i].name)

            if actual_table_columns[i].type == "FLOAT":
                try:
                    float(attr_list[i])
                except ValueError:
                    return "{} has float type.".format(actual_table_columns[i].name)

            if actual_table_columns[i].type == "BOOLEAN":
                try:
                    bool(attr_list[i])
                    if attr_list[i] not in ["TRUE", "FALSE"]:
                        return "{} has boolean type.".format(actual_table_columns[i].name)
                except ValueError:
                    return "{} has boolean type.".format(actual_table_columns[i].name)

            if actual_table_columns[i].type == "VARCHAR":
                if len(attr_list[i]) > actual_table_columns[i].length:
                    return "Varchar {} has the maximum length of {}".format(actual_table_columns[i].name,
                                                                            actual_table_columns[i].length)

        pk_idx_files = [[col, f"{fk['table']}_{fk['attribute']}"] for col, fk in table.foreign_keys.items()]
        fk_col_indexes = [column_names.index(x[0]) for x in pk_idx_files]
        fk_values = [attr_list[idx] for idx in fk_col_indexes]

        for x in pk_idx_files:
            for val in fk_values:
                existing_fk_val = db[x[1]].find_one({"_id": val})
                if existing_fk_val is None:
                    tbl = x[1].split('_')
                    return f"Value {val} of {x[0]} which references column {tbl[0]} in table {tbl[1]} does not exist"

        indexes = sorted(self.global_repo.databases[self.used_db].tables[table_name].index_files,
                         key=lambda x: x.is_unique, reverse=True)

        try:
            is_duplicate_pk = collection.find_one({'_id': primary_key})
            if is_duplicate_pk:
                return 'There is already a row with this primary key'
        except Exception as e:
            return e

        if len(indexes) > 0:
            for idx_file in indexes:
                idx_attr_pos = [column_names.index(idx_file.attributes[i]) for i in range(len(idx_file.attributes))]
                key = '$'.join(attr_list[i] for i in idx_attr_pos)
                value = '$'.join(primary_key)
                idx_file_collection = db[idx_file.name]

                try:
                    existing_idx_file = idx_file_collection.find_one({"_id": key})
                    if idx_file.is_unique == 0:
                        if existing_idx_file is None:
                            idx_file_collection.insert_one({'_id': key, 'value': value})
                        else:
                            new_value = f"{existing_idx_file['value']}#{value}"
                            idx_file_collection.update_one({'_id': key}, {'$set': {'value': new_value}})

                    elif idx_file.is_unique == 1:
                        if existing_idx_file is None:
                            idx_file_collection.insert_one({'_id': key, 'value': value})
                        else:
                            return 'Insert rejected, unique key already exists!'
                except DuplicateKeyError as e:
                    return str(e.details.get('errmsg'))
                except Exception as e:
                    return e

        try:
            attr_list.pop(primary_key_pos)
            collection.insert_one({'_id': primary_key, 'value': '#'.join(attr_list)})
        except Exception as e:
            return e
        return "Inserted one row into table {}.".format(table_name)

    def parse_delete(self, sql):
        if self.used_db is None:
            return "Please use a database first."

        table_name = sql[2]
        _id = sql[4].split('=')[1]

        if table_name not in self.global_repo.databases[self.used_db].tables.keys():
            return "The table you want to delete from does not exist."

        db = self.global_repo.mongo_client[self.used_db]
        collection = db[table_name]

        for tbl in self.global_repo.databases[self.used_db].tables.values():
            if tbl.name != table_name:
                for col, fk in tbl.foreign_keys.items():
                    references = fk['table']
                    if references == table_name:
                        for idx in tbl.index_files:
                            idx_file_name = f"{tbl.name}_{col}"
                            if idx.name == idx_file_name:
                                used = db[idx_file_name].find_one({"_id": _id})
                                if used is not None:
                                    return ('Cannot delete, id "{}" is used in child table "{}".'
                                            .format(_id, tbl.name))

        response = collection.delete_one({'_id': _id})

        if response.deleted_count == 1:
            return "Deleted the row with id {} from table {}.".format(_id, table_name)
        return "There is no entry with id {}.".format(_id)
