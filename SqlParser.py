import re
from model.Attribute import Attribute
from model.Table import Table
from model.DataBase import DataBase


class SqlParser:
    def __init__(self):
        self.used_db = None
        self.databases = []

    @staticmethod
    def cleanup_command(command):
        """removes whitespaces and capitalizes the string for easier processing"""
        command = re.sub(" +", " ", command).strip()
        command = command.upper().split()

        return command

    def parse_create_table(self, sql):
        """Parses a sql create table command and returns the new Table object
        @return: Table
        """

        words = SqlParser.cleanup_command(sql)
        table_name = words[words.index("TABLE") + 1]

        table = Table(table_name)

        attr_defs = sql[sql.index("(") + 1: sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",\n")]

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
                    continue
                elif "REFERENCES" in attr:
                    attr_info = re.findall(r"\w+", attr.split("REFERENCES")[0])
                    attr_name = attr_info[0]
                    attr_type = attr_info[1]
                    attr_length = attr_info[2] if len(attr_info) > 2 else None
                    foreign_key = re.findall(r"\w+", attr.split("REFERENCES")[1])
                    foreign_key = {"table": foreign_key[0], "attribute": foreign_key[1]}
                    table.foreign_keys[attr_name] = foreign_key
                    new_attribute = Attribute(
                        name=attr_name, type=attr_type, length=attr_length, is_null=is_null
                    )
                    table.attributes.append(new_attribute)
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

        if self.used_db is None:
            return "Please use a database first."

        for db in self.databases:
            if db.name == self.used_db:
                db.tables.append(table)
                break

        return table

    def parse_create_database(self, sql):
        words = SqlParser.cleanup_command(sql)
        if len(words) != 3:
            return "Incorrect syntax."
        db_name = words[words.index("DATABASE") + 1]
        for db in self.databases:
            if db.name == db_name:
                return "The database name is taken."
        self.databases.append(DataBase(db_name))
        return "{} database created successfully.".format(db_name)

    def parse_use_database(self, sql):
        words = SqlParser.cleanup_command(sql)
        if len(words) != 3:
            return "Incorrect syntax."
        db_name = words[words.index("DATABASE") + 1]
        for db in self.databases:
            if db.name == db_name:
                self.used_db = db_name
                return "The used database is {}.".format(self.used_db)
        return "The database you want to use does not exist."
