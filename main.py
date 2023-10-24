import json
from persistancy.GlobalRepository import GlobalRepository
from model.DataBase import DataBase
from model.Table import Table
from model.Attribute import Attribute


if __name__ == "__main__":
    sql = """CREATE TABLE students (
    StudID int PRIMARY KEY,
    GroupId int REFERENCES groups(GroupID),
    StudName varchar(20),
    Email varchar(20)
    )"""

    
    # sql2 = """CREATE TABLE marks (
    # StudID int(10) REFERENCES students(StudID),
    # DiscID varchar(20) REFERENCES disciplines(DiscID),
    # Mark int NOT NULL,
    # PRIMARY KEY (StudID,DiscID)
    # )"""

    repo = GlobalRepository()
    
    testdb = DataBase("TEST")
    
    testAttr = Attribute("id", "int", 10)
    
    table = Table("test_table")
    table.attributes.append(testAttr)
    table.primary_key.extend("id")
    
    testdb.tables["test_table"] = table
    repo.create_database(testdb)
    
