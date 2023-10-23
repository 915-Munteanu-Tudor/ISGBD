import json
from SqlParser import SqlParser

if __name__ == "__main__":
    sql = """CREATE TABLE students (
    StudID int PRIMARY KEY,
    GroupId int REFERENCES groups(GroupID),
    StudName varchar(20),
    Email varchar(20)
    )"""

    table = SqlParser().parse_create_table(sql)

    table_dict = table.to_dict()
    json_str = json.dumps(table_dict, indent=4)
    print(json_str)
