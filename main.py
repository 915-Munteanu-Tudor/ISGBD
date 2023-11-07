import json
from persistancy.GlobalRepository import GlobalRepository
from model.DataBase import DataBase
from model.Table import Table
from model.Attribute import Attribute

if __name__ == "__main__":
    # sql = """CREATE TABLE students (
    # StudID int PRIMARY KEY,
    # GroupId int REFERENCES groups(GroupID),
    # StudName varchar(20),
    # Email varchar(20)
    # )"""

    # sql2 = """CREATE TABLE marks (
    # StudID int(10) REFERENCES students(StudID),
    # DiscID varchar(20) REFERENCES disciplines(DiscID),
    # Mark int NOT NULL,
    # PRIMARY KEY (StudID,DiscID)
    # )"""

    # create database test1
    # create database test2
    # use database test1
    # create table tst1 (id int primary key)
    # create table tst2 (id int primary key, xid int references tst1(id), ag varchar(25) not null)
    # create table tst3 (id int primary key)
    # create index idx on tst2 (ag)
    # use database test2
    # create table tst1 (id int primary key, agg varchar(25))
    # drop database test2
    # drop table tst3

    repo = GlobalRepository()

    testdb = DataBase("TEST")

    testAttr = Attribute("id", "int", 10)

    table = Table("test_table")
    table.attributes.append(testAttr)
    table.primary_key.extend("id")

    testdb.tables["test_table"] = table
    repo.create_database(testdb)
