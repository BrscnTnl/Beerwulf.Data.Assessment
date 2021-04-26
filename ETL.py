import zipfile
import shutil
import pyodbc
import pandas as pd
import csv
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler

extractionPath = "data/"
filename = "data.zip"

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ASUSNB;'
                      'Database=BEERWULF;'
                      'Trusted_Connection=yes;')


def extract_zip(filename,extractionPath):
    with zipfile.ZipFile(filename,"r") as zip_ref:
        zip_ref.extractall(extractionPath)

def clean_up_the_mess(extractionPath):
    if(os.path.exists(extractionPath)):
        shutil.rmtree(extractionPath)
    else:
        print("File not found in the directory")

def clean_up_tables():
    cursor = conn.cursor()
    cursor.execute("DELETE FROM LINEITEM")
    cursor.execute("DELETE FROM ORDERS")
    cursor.execute("DELETE FROM SEGMENT")
    cursor.execute("DELETE FROM CUSTOMER")
    cursor.execute("DELETE FROM PARTSUPP")
    cursor.execute("DELETE FROM SUPPLIER")
    cursor.execute("DELETE FROM NATION")
    cursor.execute("DELETE FROM REGION")
    cursor.execute("DELETE FROM PART")
    
def ETL_REGION():
    with open ('data/region.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into REGION values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_PART():
    with open ('data/part.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into PART values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_NATION():
    with open ('data/nation.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into NATION values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_SUPPLIER():
    with open ('data/supplier.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into SUPPLIER values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_PARTSUPP():
    with open ('data/partsupp.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into PARTSUPP values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_CUSTOMER():
    with open ('data/customer.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into CUSTOMER values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_ORDERS():
    with open ('data/orders.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into ORDERS values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def ETL_LINEITEM():
    with open ('data/lineitem.tbl', 'r', newline='') as f:
        reader = csv.reader(f, delimiter='|')
        data = next(reader) 
        query = 'insert into LINEITEM values ({0})'
        query = query.format(','.join('?' * len(data[:-1])))
        cursor = conn.cursor()
        cursor.execute(query, data[:-1])
        for data in reader:
            cursor.execute(query, data[:-1])
        cursor.commit()

def CustomerSegmentation():
    sql = "Select * From W_ClusteringData "
    data = pd.read_sql(sql,conn)
    scaler = MinMaxScaler()
    data[['QUANTITY','EXTENDEDPRICE']] = scaler.fit_transform(data[['QUANTITY','EXTENDEDPRICE']])
    kmeans = KMeans(n_clusters=3, init='k-means++', max_iter=300, n_init=10, random_state=0)
    pred_y = kmeans.fit_predict(data[['QUANTITY','EXTENDEDPRICE']])
    data['SEGMENT'] = pd.Series(pred_y, index=data.index)
    cursor = conn.cursor()
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO SEGMENT (SEG_CUSTKEY,SEG_SEGMENT) values(?,?)", row.O_CUSTKEY , row.SEGMENT)
    conn.commit()



extract_zip(filename,extractionPath)
clean_up_tables()
ETL_REGION()
ETL_PART()
ETL_NATION()
ETL_SUPPLIER()
ETL_PARTSUPP()
ETL_CUSTOMER()
ETL_ORDERS()
ETL_LINEITEM()
CustomerSegmentation()
clean_up_the_mess(extractionPath)
conn.close()