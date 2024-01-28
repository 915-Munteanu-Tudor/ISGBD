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

        attr_defs = sql[sql.index("(") + 1 : sql.rindex(")")]
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
                table.index_files.append(
                    Index(f"{table_name}_{'-'.join(keys)}", 1, keys)
                )
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
                    table.index_files.append(
                        Index(f"{table_name}_{key_name}", 0, [key_name])
                    )
                    continue
                elif "REFERENCES" in attr:
                    attr_info = re.findall(r"\w+", attr.split("REFERENCES")[0])
                    attr_name = attr_info[0]
                    attr_type = attr_info[1]
                    attr_length = attr_info[2] if len(attr_info) > 2 else None
                    foreign_key = re.findall(r"\w+", attr.split("REFERENCES")[1])
                    if (
                        foreign_key[0]
                        not in self.global_repo.databases[self.used_db].tables.keys()
                    ):
                        return "Cannot reference a nonexistent table."
                    existent_columns = [
                        col.name
                        for col in self.global_repo.databases[self.used_db]
                        .tables[foreign_key[0]]
                        .attributes
                    ]
                    if foreign_key[1] not in existent_columns:
                        return "Cannot reference a nonexistent column."
                    foreign_key = {"table": foreign_key[0], "attribute": foreign_key[1]}
                    table.foreign_keys[attr_name] = foreign_key
                    new_attribute = Attribute(
                        name=attr_name,
                        type=attr_type,
                        length=attr_length,
                        is_null=is_null,
                    )
                    table.attributes.append(new_attribute)
                    table.index_files.append(
                        Index(
                            f"{table_name}_{new_attribute.name}",
                            0,
                            [new_attribute.name],
                        )
                    )
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
                    references = fk["table"]
                    if references == table_name:
                        return 'Cannot drop table "{}" because child table "{}" references it.'.format(
                            table_name, tbl.name
                        )
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

        indexes = (
            self.global_repo.databases[self.used_db].tables[table_name].index_files
        )
        if len(indexes) > 0:
            for idx in indexes:
                if index_name == idx.name.split(".")[0]:
                    return "There is already an index with this name on the table."

        index = Index(index_name, 0)

        attr_defs = sql[sql.index("(") + 1 : sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",")]

        table_columns = [
            col.name
            for col in self.global_repo.databases[self.used_db]
            .tables[table_name]
            .attributes
        ]
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

        indexes = (
            self.global_repo.databases[self.used_db].tables[table_name].index_files
        )
        if len(indexes) > 0:
            for idx in indexes:
                if index_name == idx.name.split(".")[0]:
                    return "There is already an index with this name on the table."

        index = Index(index_name, 1)

        attr_defs = sql[sql.index("(") + 1 : sql.rindex(")")]
        attr_list = [x.strip() for x in attr_defs.split(",")]

        table_columns = [
            col.name
            for col in self.global_repo.databases[self.used_db]
            .tables[table_name]
            .attributes
        ]
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

        attr_defs = sql[sql.index("(") + 1 : sql.rindex(")")]
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
                        return "{} has boolean type.".format(
                            actual_table_columns[i].name
                        )
                except ValueError:
                    return "{} has boolean type.".format(actual_table_columns[i].name)

            if actual_table_columns[i].type == "VARCHAR":
                if len(attr_list[i]) > actual_table_columns[i].length:
                    return "Varchar {} has the maximum length of {}".format(
                        actual_table_columns[i].name, actual_table_columns[i].length
                    )

        pk_idx_files = [
            [col, f"{fk['table']}_{fk['attribute']}"]
            for col, fk in table.foreign_keys.items()
        ]
        fk_col_indexes = [column_names.index(x[0]) for x in pk_idx_files]
        fk_values = [attr_list[idx] for idx in fk_col_indexes]

        for x in pk_idx_files:
            for val in fk_values:
                existing_fk_val = db[x[1]].find_one({"_id": val})
                if existing_fk_val is None:
                    tbl = x[1].split("_")
                    return f"Value {val} of {x[0]} which references column {tbl[0]} in table {tbl[1]} does not exist"

        indexes = sorted(
            self.global_repo.databases[self.used_db].tables[table_name].index_files,
            key=lambda x: x.is_unique,
            reverse=True,
        )

        try:
            is_duplicate_pk = collection.find_one({"_id": primary_key})
            if is_duplicate_pk:
                return "There is already a row with this primary key"
        except Exception as e:
            return e

        if len(indexes) > 0:
            for idx_file in indexes:
                idx_attr_pos = [
                    column_names.index(idx_file.attributes[i])
                    for i in range(len(idx_file.attributes))
                ]
                key = "$".join(attr_list[i] for i in idx_attr_pos)
                value = "$".join(primary_key)
                idx_file_collection = db[idx_file.name]

                try:
                    existing_idx_file = idx_file_collection.find_one({"_id": key})
                    if idx_file.is_unique == 0:
                        if existing_idx_file is None:
                            idx_file_collection.insert_one({"_id": key, "value": value})
                        else:
                            new_value = f"{existing_idx_file['value']}#{value}"
                            idx_file_collection.update_one(
                                {"_id": key}, {"$set": {"value": new_value}}
                            )

                    elif idx_file.is_unique == 1:
                        if existing_idx_file is None:
                            idx_file_collection.insert_one({"_id": key, "value": value})
                        else:
                            return "Insert rejected, unique key already exists!"
                except DuplicateKeyError as e:
                    return str(e.details.get("errmsg"))
                except Exception as e:
                    return e

        try:
            attr_list.pop(primary_key_pos)
            collection.insert_one({"_id": primary_key, "value": "#".join(attr_list)})
        except Exception as e:
            return e
        return "Inserted one row into table {}.".format(table_name)

    def parse_delete(self, sql):
        if self.used_db is None:
            return "Please use a database first."

        table_name = sql[2]
        _id = sql[4].split("=")[1]

        if table_name not in self.global_repo.databases[self.used_db].tables.keys():
            return "The table you want to delete from does not exist."

        db = self.global_repo.mongo_client[self.used_db]
        collection = db[table_name]

        for tbl in self.global_repo.databases[self.used_db].tables.values():
            if tbl.name != table_name:
                for col, fk in tbl.foreign_keys.items():
                    references = fk["table"]
                    if references == table_name:
                        for idx in tbl.index_files:
                            idx_file_name = f"{tbl.name}_{col}"
                            if idx.name == idx_file_name:
                                used = db[idx_file_name].find_one({"_id": _id})
                                if used is not None:
                                    return 'Cannot delete, id "{}" is used in child table "{}".'.format(
                                        _id, tbl.name
                                    )

        response = collection.delete_one({"_id": _id})

        if response.deleted_count == 1:
            return "Deleted the row with id {} from table {}.".format(_id, table_name)
        return "There is no entry with id {}.".format(_id)

    def parse_select(self, sql):
        if self.used_db is None:
            return "Please use a database first."

        sql = SqlParser.cleanup_command(sql, False)

        is_distinct = False
        table_names = []
        attributes = []
        conditions = []

        if "distinct" in sql.lower():
            is_distinct = True

        attributes_part = re.search(
            r"SELECT\s+DISTINCT\s+(.*?)\s+FROM", sql, re.IGNORECASE
        )
        if not attributes_part:
            attributes_part = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE)
        if attributes_part:
            attributes = [attr.strip() for attr in attributes_part.group(1).split(",")]

        # Extract the table names
        tables_part = re.search(r"FROM\s+(.*?)(\s+WHERE|\s*;|$)", sql, re.IGNORECASE)
        if tables_part:
            table_names = [table.strip() for table in tables_part.group(1).split(",")]

        # Extract conditions from WHERE clause
        conditions_part = re.search(r"WHERE\s+(.*?)(\s*;|$)", sql, re.IGNORECASE)
        if conditions_part:
            conditions = [
                condition.strip()
                for condition in conditions_part.group(1).split(" AND ")
            ]

        existentTableNames = self.global_repo.databases[self.used_db].tables.keys()
        for table in table_names:
            if table not in existentTableNames:
                return "The table(s) you want to select from does not exist."

        # Step 1
        primary_keys, remaining_conditions = self.fetch_primary_keys_using_indexes(
            table_names, conditions
        )

        # Step 2: Fetch records based on primary keys
        records = self.fetch_and_filter_records(
            primary_keys, table_names, remaining_conditions
        )

        # Step 3: Apply DISTINCT if necessary
        if is_distinct:
            records = self.apply_distinct(records, attributes)

        # Step 4: Format and return results
        formatted_results = self.format_results(records, attributes)
        return formatted_results

    def fetch_and_filter_records(
        self, primary_keys, tables_involved, remaining_conditions
    ):
        filtered_records = []
        db = self.global_repo.mongo_client[self.used_db]

        # Step 1: Fetch records for each table
        table_records = {}
        for table_name in tables_involved:
            collection = db[table_name]
            if primary_keys:
                table_records[table_name] = list(
                    collection.find({"_id": {"$in": list(primary_keys)}})
                )
            else:
                table_records[table_name] = list(collection.find({}))

        # Step 2: Identify join conditions and separate them from other conditions
        join_conditions, other_conditions = self.separate_join_conditions(
            remaining_conditions
        )

        # Step 3: Implement join logic (simplified example for INNER JOIN)
        if len(tables_involved) > 1 and join_conditions:
            # Simplified join logic, assuming only one join condition for illustration
            join_attr, join_val = join_conditions[0].split("=")
            table1, attr1 = join_attr.split(".")
            table2, attr2 = join_val.split(".")

            for rec1 in table_records[table1]:
                for rec2 in table_records[table2]:
                    if rec1[attr1] == rec2[attr2]:
                        # Combine records from both tables
                        combined_rec = {**rec1, **rec2}
                        if self.record_meets_conditions(combined_rec, other_conditions):
                            filtered_records.append(combined_rec)
        else:
            # No joins, simply apply remaining conditions
            for table_name, records in table_records.items():
                for record in records:
                    deserialized_record = self.deserialize_record(record, table_name)
                    if self.record_meets_conditions(
                        deserialized_record, remaining_conditions
                    ):
                        filtered_records.append(deserialized_record)

        return filtered_records

    def deserialize_record(self, record, table_name):
        deserialized_record = {}
        record_values = [record["_id"]]
        record_values.extend(record["value"].split("#"))

        for i, attribute in enumerate(
            self.global_repo.databases[self.used_db].tables[table_name].attributes
        ):
            deserialized_record[attribute.name] = record_values[i]

        return deserialized_record

    def separate_join_conditions(self, conditions):
        # Logic to separate join conditions from other conditions
        join_conditions = []
        other_conditions = []
        for condition in conditions:
            if (
                "." in condition
            ):  # Assuming join conditions involve table.attribute syntax
                join_conditions.append(condition)
            else:
                other_conditions.append(condition)
        return join_conditions, other_conditions

    def record_meets_conditions(self, record, conditions):
        for condition in conditions:
            attribute, operator, value = self.parse_condition(condition)
            # Split attribute to get table name and attribute name if needed
            table_name, attr_name = self.split_attribute(attribute)
            attr_type = self.get_attribute_type(table_name, attr_name)

            if not self.evaluate_condition(
                record[attr_name], attr_type, operator, value
            ):
                return False
        return True

    def split_attribute(self, attribute):
        if "." in attribute:
            table_name, attr_name = attribute.split(".")
        else:
            table_name = None
            attr_name = attribute
        return table_name, attr_name

    def get_attribute_type(self, table_name, attribute):
        if table_name:
            return (
                self.global_repo.databases[self.used_db]
                .tables[table_name]
                .attributes[attribute]
                .type
            )
        else:
            # If no table name is specified, find the attribute in the involved tables
            for table in self.global_repo.databases[self.used_db].tables.values():
                for table_attribute in table.attributes:
                    if attribute == table_attribute.name:
                        return table_attribute.type
        raise ValueError(f"Attribute type for {attribute} not found")

    def format_results(self, records, attributes):
        # Create the header row
        header = "\t".join(attributes)
        formatted_rows = [header]

        for record in records:
            # Create a row for each record
            row = "\t".join([record.get(attr, "") for attr in attributes])
            formatted_rows.append(row)

        # Combine all rows into a single string
        formatted_result = "\n".join(formatted_rows)
        return formatted_result

    # ============= index based opperations
    def get_index(self, table_name, attribute):
        for index in (
            self.global_repo.databases[self.used_db].tables[table_name].index_files
        ):
            if attribute in index.attributes:
                return index

        return None

    def fetch_keys_from_index(self, is_unique, table_name, attribute, operator, value):
        db = self.global_repo.mongo_client[self.used_db]
        index_collection = db[f"{table_name}_{attribute}"]

        # TODO: fix mongo query to check based on type not literal string
        query = {f"_id": {self.translate_to_mongo_operator(operator): value}}
        results = index_collection.find(query)

        primary_keys = []
        for result in results:
            if is_unique:
                primary_keys.append(result["value"].replace("$", ""))
            else:
                primary_keys.extend(result["value"].replace("$", "").split("#"))

        return primary_keys

    def fetch_primary_keys_using_indexes(self, table_names, conditions):
        primary_keys_per_condition = []

        used_conditions = []
        join_conditions, other_conditions = self.separate_join_conditions(conditions)

        for condition in conditions:
            attribute, operator, value = self.parse_condition(condition)
            used_condition = False
            for table_name in table_names:
                idx = self.get_index(table_name, attribute)
                if idx is not None:
                    primary_keys = self.fetch_keys_from_index(
                        idx.is_unique, table_name, attribute, operator, value
                    )
                    primary_keys_per_condition.append(primary_keys)
                    used_condition = True

            if used_condition:
                used_conditions.append(condition)

        if primary_keys_per_condition:
            intersected_primary_keys = set.intersection(
                *map(set, primary_keys_per_condition)
            )
        else:
            intersected_primary_keys = set()

        remaining_conditions = [
            condition for condition in conditions if condition not in used_conditions
        ]
        return intersected_primary_keys, remaining_conditions

    # ============ utils
    def parse_condition(self, condition):
        parts = condition.split(" ")
        attribute = parts[0].strip()
        operator = parts[1].strip()
        value = parts[2].strip().strip("'").strip('"')
        return attribute, operator, value

    def translate_to_mongo_operator(self, operator):
        operator_mapping = {
            "=": "$eq",
            ">": "$gt",
            "<": "$lt",
            ">=": "$gte",
            "<=": "$lte",
        }
        return operator_mapping.get(operator, "$eq")

    def evaluate_condition(self, record_value, attribute_type, operator, value):
        # Convert value to appropriate type based on attribute_type
        if attribute_type == "INT":
            record_value = int(record_value)
            value = int(value)
        elif attribute_type == "FLOAT":
            record_value = float(record_value)
            value = float(value)
        elif attribute_type == "BOOLEAN":
            record_value = record_value.lower() in ["true", "1", "t", "y", "yes"]
            value = value.lower() in ["true", "1", "t", "y", "yes"]
        elif attribute_type == "VARCHAR":
            # No conversion needed for strings, but ensure both are strings
            record_value = str(record_value)
            value = str(value)

        # Operator-based evaluation
        if operator in ["=", ">", "<", ">=", "<="]:
            if attribute_type not in ["INT", "FLOAT", "VARCHAR"]:
                raise ValueError("Invalid operator for type BOOLEAN")
            return self.compare_values(record_value, operator, value)
        elif operator == "LIKE":
            if attribute_type != "VARCHAR":
                raise ValueError("LIKE operator is only valid for VARCHAR type")
            regex_pattern = "^" + value.replace("%", ".*").replace("_", ".") + "$"
            return re.match(regex_pattern, record_value) is not None
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def compare_values(self, record_value, operator, value):
        # Comparison for '=', '>', '<', '>=', '<='
        if operator == "=":
            return record_value == value
        elif operator == ">":
            return record_value > value
        elif operator == "<":
            return record_value < value
        elif operator == ">=":
            return record_value >= value
        elif operator == "<=":
            return record_value <= value

    def apply_distinct(self, records, attributes):
        unique_records = []
        seen_combinations = set()

        for record in records:
            # Create a tuple of values for the specified attributes
            attribute_values = tuple(
                record[attr] for attr in attributes if attr in record
            )

            # Check if this combination of values has already been seen
            if attribute_values not in seen_combinations:
                seen_combinations.add(attribute_values)
                unique_records.append(record)

        return unique_records
