import json
from SqlParser import SqlParser

if __name__ == "__main__":
    sql = """CREATE TABLE students (
    StudID int PRIMARY KEY,
    GroupId int REFERENCES groups(GroupID),
    StudName varchar(20),
    Email varchar(20)
    )"""
    
    sql2 = """CREATE TABLE marks (
    StudID int(10) REFERENCES students(StudID),
    DiscID varchar(20) REFERENCES disciplines(DiscID),
    Mark int,
    PRIMARY KEY (StudID,DiscID)
    )"""

    table = SqlParser().parse_create_table(sql2)

    table_dict = table.to_dict()
    json_str = json.dumps(table_dict, indent=4)
    print(json_str)
