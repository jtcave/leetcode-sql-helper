#!/usr/bin/env python3

import json
import os
import sqlite3

def read_json_stdin():
    json_ob = input()
    ob = json.loads(json_ob)
    return ob

def rotate_tables(testcase):
    """Rearranges the testcase format into a dict of table blocks.
    Each dict entry's key is the table name, each value is a pair of
    (column_headers, rows)"""
    headers = testcase['headers']
    rows = testcase['rows']
    table_names = headers.keys()
    return {k: (headers[k], rows[k]) for k in table_names}

def sql_template(headers):
    if len(headers) == 0:
        creator = "()"
        inserter = "()"
    elif len(headers) == 1:
        creator = f"('{headers[0]}')"
        inserter = "(?)"
    else:
        creator_inner = ','.join(f"'{x}'" for x in headers)
        creator = f"({creator_inner})"
        inserter_inner = ','.join('?' for _ in headers)
        inserter = f"({inserter_inner})"
    return creator, inserter

def do_sql(cur, sql, *args):
    print(sql, args)
    return cur.execute(sql, *args)
        
def store_tables(table_blocks, db_conn):
    for table_name, (headers, rows) in table_blocks.items():
        cur = db_conn.cursor()
        creator, inserter = sql_template(headers)
        do_sql(cur, f"DROP TABLE IF EXISTS {table_name};")
        do_sql(cur, f"CREATE TABLE {table_name} {creator};")
        insert_query = f"INSERT INTO {table_name} VALUES {inserter};"
        for row in rows:
            do_sql(cur, insert_query, row)
        db_conn.commit()
        print()

def main():
    filename = input("Filename: ")
    
    print("Paste the test case:")
    test_case = read_json_stdin()
    test_data = rotate_tables(test_case)

    print()
    db_conn = sqlite3.connect(filename)
    store_tables(test_data, db_conn)
    db_conn.close()

    print()
    print("Invoking sqlite3")
    os.system(f"sqlite3 -table {filename}")

if __name__ == "__main__":
    main()
