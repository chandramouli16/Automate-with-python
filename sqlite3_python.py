#this python script uses sqlite3 package to creates a db file based on given options and performs basic db operations 
# %%
"""
Sqlite3 package
"""

# %%
import sqlite3
import pandas as pd
import tkinter as tk
from tkinter import filedialog
from prettytable import PrettyTable

# %%
def getfile():
    root = tk.Tk()
    root.withdraw()
    path =filedialog.askopenfilenames()[0]
    return path

# %%
def source():
    global c
    global conn
    print("1) From excel or csv file")
    print("2) .db file ")
    print("3) In RAM memory(temporary) ")
    source=int(input("select db source :  "))
    if source==1:
        try:
            path=getfile()
        except:
            source()
        if (path.split('/')[-1]).split('.')[1] not in ["xlsx","csv"]:
            print(r"\n Select only xlsx or csv file")
            try:
                path=getfile()
            except:
                source()
        print("File : ",path)
        filename=(path.split('/')[-1]).split('.')[0]
        ext = (path.split('/')[-1]).split('.')[1]
        try:
            conn=sqlite3.connect(filename+".db")
            if ext == 'xlsx':
                wb=pd.read_excel(path,sheet_name=None)
                for sheet in wb:
                    wb[sheet].to_sql(sheet,conn, index=False)
            elif ext == 'csv':
                wb=pd.read_csv(path)
                wb.to_sql(filename,conn,index=False)
            conn.commit()
            print("")
            print("Converted file into db file ")
            print("")
        except:
            print("Connection failed ")
    elif source==2:
        print("Select .db file ")
        try:
            path=getfile()
        except:
            source()
        if (path.split('/')[-1]).split('.')[1] !="db":
            print("Select only .db file")
            try:
                path=getfile()
            except:
                source()
        print("File : ",path)
        
        try:
            filename=(path.split('/')[-1]).split('.')[0]
            conn=sqlite3.connect(filename+".db")
            print("Loaded the "+filename+".db")
        except:
            print("Connection failed ")
    elif source==3:
        try:
            conn = sqlite3.connect(":memory:")
            print('created db in memory ')
        except:
            print("Connection failed ")
        
    else:
        print("Enter valid opt ")
        print("")
        source()

    c = conn.cursor()
    flow()

# %%
def flow():
    print("")
    f=input("Want to continue : [y/n] ")
    if f=='y':
        main()
    elif f=='n':
        conn.commit()
        print("Disconnecting...")
        c.close()
        exit()
    else:
        print("Enter valid opt ")
        print("")
        flow()

# %%
#display all tables present in database
def show_tables():
    c.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%'")
    result = c.fetchall()
    for row in result:
        print(row[0])
    if len(result)==0:
        print('')
        print("No table exist")
        flow()
    print("")

# %%
#to check the existence of the table
def check_table(tn):    
    c.execute("SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%'")
    result = c.fetchall()
    temp=[]
    for row in result:
        temp.append(row[0])
    if tn not in temp:
        print('')
        print("Table does not exist")
        flow()
    print('')

# %%
#create table query
def create():
    table_name = input('Enter table name : ')
    no_of_columns=int(input("Enter number of columns : "))
    columns =[]
    for i in range(1,no_of_columns+1):
        columns.append(input('Enter column {} name and datatype(with space) : '.format(i)))
    query = "create table {tname}({column})"
    sql_command = query.format(tname=table_name,column =",".join(columns))
    try:
        c.execute(sql_command)
        print('')
        print("Table created")
    except sqlite3.Error as e:
        print("An error occurred:", e.args[0])
    print("")
    flow()

# %%
#drop table query
def drop():
    print("list of tables : ")
    show_tables()
    table_name = input('Enter table name : ')
    check_table(table_name)
    query = "drop table {tname}"
    sql_command = query.format(tname=table_name) 
    try:
        c.execute(sql_command)
        print("Table dropped")
    except sqlite3.Error as e:
        print("An error occurred:", e.args[0])
    print("")
    flow()

# %%
#to insert records into table
def insert():
    table_name = input("Enter table name : ")
    check_table(table_name)
    no_of_rows = int(input("How many records : "))
    rows=[]
    columnlist=show_columns(table_name)
    for cl in columnlist:
        if " " in cl:
            columnlist[columnlist.index(cl)]='"'+cl+'"'
    print("Enter values corresponding to ",columnlist)
    for i in range(1,no_of_rows+1):
        temp=[]
        temp=tuple(convert(x) for x in input().split(','))
        rows.append(temp)
    query = "insert into {tname}({clist})values({vlist})"
    sql_command = query.format(tname=table_name,clist=','.join(columnlist),vlist=','.join('?'*len(columnlist)))
    try:
        c.executemany(sql_command,(rows))
        print("")
        print("records inserted")
    except sqlite3.Error as e:
        print("An error occurred:", e.args[0])
    print("")
    flow()

# %%
#to display all columns of table
def show_columns(tn):
    table_name = tn
    query = "select * from {tname} limit 1"
    sql_command = query.format(tname=table_name)
    c.execute(sql_command)
    cl_names = [description[0] for description in c.description]
    return cl_names

# %%
#to convert all the values from user (insert cmd) into integer or string 
def convert(x):
    try :
        return int(x)
    except:
        return str(x)

# %%
#does select * operation
def show():
    table_name = input("Enter table name : ")
    check_table(table_name)
    query = "select * from {tname}"
    sql = query.format(tname=table_name)
    try:
        c.execute(sql)
        result = c.fetchall()
        if len(result)==0:
            print("No records found ")
        '''for row in result:
            print(row)'''
        t = PrettyTable(show_columns(table_name))
        for row in result:
            t.add_row(row)
        print(t)
    except sqlite3.Error as e:
        print("An error occurred:", e.args[0])
    print("")
    flow()

# %%
#deleting records based on conditions
def delete():
    table_name = input("Enter table name : ")
    check_table(table_name)
    col_list=show_columns(table_name)
    print('Columns are : ',col_list)
    cn=input("Enter condition (eg empid=1) : ")
    query = "delete from {tname} where {con}"
    sql = query.format(tname=table_name,con=cn)
    try:
        c.execute(sql)
        print("")
        print('Records deleted')
    except sqlite3.Error as e:
        print("An error occurred:", e.args[0])
    print('')
    flow()
    

# %%
#customised query from user
def exp():
    conn.isolation_level = None

    buffer = ""

    print("Enter your SQL commands to execute in sqlite3.")
    print("Enter a blank line to exit.")

    while True:
        line = input()
        if line == "":
            break
        buffer += line
        if sqlite3.complete_statement(buffer):
            try:
                buffer = buffer.strip()
                c.execute(buffer)

                if buffer.lstrip().upper().startswith("SELECT"):
                    print(c.fetchall())
            except sqlite3.Error as e:
                print("An error occurred:", e.args[0])
            buffer = ""
    print("")
    flow()


# %%
def main():
    print("List of Operations ")
    print("1) Create a table ")
    print("2) Show all tables present in db ")
    print("3) Show all records present in table ")
    print("4) Insert values in table ")
    print("5) Drop table ")
    print("6) Show all columns present in table ")
    print("7) Delete records from table ")
    print("8) Write your own query ")
    print("")
    choice=int(input("Choose the operation to perform : "))
    print("")
    if choice==1:
        create()
    elif choice==2:
        show_tables()
    elif choice==3:
        show()
    elif choice==4:
        insert()
    elif choice==5:
        drop()
    elif choice==6:
        table_name = input("Enter table name : ")
        check_table(table_name)
        col_list=show_columns(table_name)
        print('Columns are : ',col_list)
    elif choice==7:
        delete()
    elif choice==8:
        exp()
    else:
        print("Enter valid choice")
        print("")
        main()
    print("")
    flow()
    

# %%
source()
