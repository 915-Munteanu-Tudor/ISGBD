import json
import time
from Client import Client
# from Server import Server
from persistancy.GlobalRepository import GlobalRepository
from model.DataBase import DataBase
from model.Table import Table
from model.Attribute import Attribute

import random
import string

def generate_random_string(length):
    return ''.join(random.choice(string.ascii_letters) for _ in range(length))

def create_insert_sql(table_name, user_id, username, age, email):
    return f"INSERT INTO {table_name} VALUES ({user_id}, {username}, {age}, {email});"


if __name__ == "__main__":    
    # server = Server("localhost", 8081)

    # number_of_records = 1000000
    table_name_indexed = 'Users_Indexed'
    table_name_non_indexed = 'Users_NonIndexed'
    
    # server.execute_command("USE DATABASE TEST2")

    # for i in range(2596, number_of_records):
    #     username = f'user_{i}'
    #     age = random.randint(18, 100)
    #     email = f'user{i}@example.com'

    #     # Generate SQL for table with index
    #     sql_indexed = create_insert_sql(table_name_indexed, i, username, age, email)
    #     response = server.execute_command(sql_indexed)
    #     print(f"Insert Indexed: {response}")

    #     # Generate SQL for table without index
    #     sql_non_indexed = create_insert_sql(table_name_non_indexed, i, username, age, email)
    #     response = server.execute_command(sql_non_indexed)
    #     print(f"Insert Non-Indexed: {response}")
    
    
    client = Client("localhost", 8081)
    
    last_record_id = 1500
    
    client.send_command("USE DATABASE TEST2")
    start_time = time.time()
    client.send_command(f'SELECT USERID,USERNAME,AGE,EMAIL FROM {table_name_indexed} WHERE EMAIL = USER{last_record_id}@EXAMPLE.COM')
    print(f"Query on Indexed Table took: {time.time() - start_time} seconds")

    start_time = time.time()
    client.send_command(f'SELECT USERID,USERNAME,AGE,EMAIL FROM {table_name_non_indexed} WHERE EMAIL = USER{last_record_id}@EXAMPLE.COM')
    print(f"Query on Non-Indexed Table took: {time.time() - start_time} seconds")
    
    # for 1.500 records
    # Query on Indexed Table took: 0.2867288589477539 seconds
    # Query on Non-Indexed Table took: 0.5760180950164795 seconds
    
    # for 2.500 records
    # Query on Indexed Table took: 0.26223015785217285 seconds
    # Query on Non-Indexed Table took: 0.6615841388702393 seconds
