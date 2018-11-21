#!/usr/bin/python
import json
import sqlite3
from sqlite3 import Error
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import AsIs

from datetime import datetime as dt
from datetime import timedelta, date

relation_cygnus_dit = [{
                        'dit_id':73, 
                        'table_cygnus': "sc_malaga.malaga_kpis_roomtest3_room",
                        'postgres_connection': "dbname='cedus' user='cedus' host='localhost' password='cedus'",
                        'sql_database_path': "/home/ubuntu/CEDUS/BE/pcompass.db"
                        }]


#create db connexion sqlite
def create_connection_sqlite(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        connSqlLite = sqlite3.connect(db_file)
        return connSqlLite
    except Error as e:
        print(e)
 
    return None


#create db connexion postgres
def create_connection_postgres(connect_str):
    """ create a database connection to the postgres database
        specified by the db_file
    :return: Connection object or None
    """
    try:
        #connect_str = "dbname='cedus' user='cedus' host='localhost' password='cedus'"
        # use our connection values to establish a connection
        connPostGres = psycopg2.connect(connect_str)
        
        return connPostGres
    except Error as e:
        print(e)
 
    return None



#conn = sqlite3.connect('/home/ubuntu/CEDUS/BE/pcompass.db')

#c = conn.cursor()

#t = ('73',)
#c.execute('SELECT * FROM datasetmanager_dataset WHERE id=?', t)
#print c.fetchone()

def select_data_postgres(conn, table_name):
    
    print("Select Data From Postgress DB, table:"+str(table_name))
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    #cursor.execute("""SELECT * from sc_malaga.malaga_kpis_roomtest3_room""")
    cursor.execute("""select date(recvtime) as formatDate, avg(CAST(attrvalue as FLOAT)) as attrvalue, attrname from %s where attrname='temperature'  group by date(recvtime), attrname order by formatDate;""",(AsIs(table_name),))
    rows = cursor.fetchall()
    #print(rows)
    
    
    return (rows)


def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
def update_dataset(conn, item, dataset_id):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param item:
    :return:
    """
    print("item")
    print (item)
    #dataset_id = 73
    
    print (str(dataset_id))
    cur = conn.cursor()
    #cur.execute("SELECT * FROM datasetmanager_dataset WHERE id=?", (dataset_id,))
    cur.execute("SELECT time_end, time_start, time_resolution, data FROM datasetmanager_dataset WHERE id=?", (dataset_id,))
    
    rows = cur.fetchall()
    for row in rows:

        print ("dataset time_end")
        print(row[0])
        print ("time input")
        dataIn = item['formatdate']
        print (dataIn)
        
        print ("data")
        print(row[3])
               
        new_end_date = row[0]
        
        a = dt.strptime(row[0], "%Y-%m-%d")
        b = dt.strptime(str(dataIn), "%Y-%m-%d")
        
        if (b>a):
            new_end_date = dataIn
        
        print("new_end_date")
        print(new_end_date)
        
        d = json.loads(row[3])
        print("json data_frame")
        print(d['data_frame'])
        
        jsonDataFrame = json.loads(d['data_frame'])

        for dataFrame  in jsonDataFrame:
            row_id = dataFrame
            
            print("row_id")
            print(row_id)
            print(jsonDataFrame[dataFrame])
            individualData = jsonDataFrame[dataFrame]
            updateValue = False
            
            '''
            for individualDataByTimeStamp  in individualData:
                print ("time:")
                print (individualDataByTimeStamp)
                print ("value:")
                print (individualData[individualDataByTimeStamp])
                dateInToCompare = str(dataIn)+"T00:00:00.000Z"
                
                if (individualDataByTimeStamp == dateInToCompare):
                    print("Update Value")
                    updateValue = True
                    break
            '''    
            
            new_date = str(dataIn)+'T00:00:00.000Z'
            new_data = float(item['attrvalue']);

            jsonDataFrame[dataFrame][new_date] = new_data
            
            print ("final json updated")
            print(jsonDataFrame[dataFrame])
            
        #fill gaps if is necessary
        print ("time_start")
        print(row[0])
        print ("time_end")
        print(row[1])
        print ("time_resolution")
        print(row[2])
        
        if (row[2]=='day'):
            print ("dins!!")
            array_start_date = str(row[1])
            array_start_date = array_start_date.split("-")
            
            array_end_date = str(new_end_date)
            array_end_date = array_end_date.split("-")
            
            start_date = date(int(array_start_date[0]), int(array_start_date[1]), int(array_start_date[2]))
            end_date = date(int(array_end_date[0]), int(array_end_date[1]), int(array_end_date[2]))

            for single_date in daterange(start_date, end_date):
                #print (single_date.strftime("%Y-%m-%d"))  
                new_date_to_check =  single_date.strftime("%Y-%m-%d")+'T00:00:00.000Z'
                
                
                if (new_date_to_check in jsonDataFrame[dataFrame]):
                    #print (new_date_to_check)
                    #print("existe")
                    #print (jsonDataFrame[dataFrame][new_date_to_check])
                    if (jsonDataFrame[dataFrame][new_date_to_check]== None):
                        jsonDataFrame[dataFrame][new_date_to_check] = 'null'
                else:
                    #jsonDataFrame[dataFrame][new_date_to_check] = None
                    #jsonDataFrame[dataFrame][new_date_to_check] = 0
                    jsonDataFrame[dataFrame][new_date_to_check] = 'null'

        
        print (" super final json updated")
        print(jsonDataFrame[dataFrame])    
        
        print ("row_id")
        print (row_id)
        new_row_json = {}
        new_row_json[row_id] = jsonDataFrame[dataFrame]        
        
        new_row_json_string = str(new_row_json)
        
        
        new_row_json_string = new_row_json_string.replace("'", '\\"')
        
        
        
        final_json = {"data_frame": new_row_json_string, "resolution": d['resolution'], "unit": d['unit']}
        print("final_json")
        print(final_json)
        final_json_as_string = str(final_json)
        final_json_as_string = final_json_as_string.replace("'", '"')
        final_json_as_string = final_json_as_string.replace("\\\\", "\\")


        final_json_as_string = final_json_as_string.replace('\\"null\\"', "null")

        print("final_json_as_string")
        print(final_json_as_string)


        print("new_end_date")
        print(new_end_date)

        cur.execute("UPDATE datasetmanager_dataset SET time_end=?, data=? WHERE id=?", (new_end_date, final_json_as_string, dataset_id,))
        
        
def main():
    
    print ("Start!!!")
    
    for element in relation_cygnus_dit:
        print("-------- start sincro -------")
        print("dit_id:"+str(element['dit_id']))
        print("table_cygnus:"+str(element['table_cygnus']))
        print("postgres_connection:"+str(element['postgres_connection']))
        print("sql_database_path:"+str(element['sql_database_path']))

        # create a database sqlite connection
        #connect_str = "dbname='cedus' user='cedus' host='localhost' password='cedus'"
        connect_str = element['postgres_connection']
        connPostgres = create_connection_postgres(connect_str)
    
        # create a database sqlite connection
        #database = "/home/ubuntu/CEDUS/BE/pcompass.db"
        database = element['sql_database_path']
        connSqlLite = create_connection_sqlite(database)    
    
    
    
        with connPostgres:
            print("1. Query tablesPostGres  ")
            dataPostGresRec = select_data_postgres(connPostgres, element['table_cygnus'])
            
            print("dataPostGresRec")
            print(dataPostGresRec)
            
            for item  in dataPostGresRec:
                print ("item")
                valueToInsert = item['attrvalue']
                attributeToInsert = item['attrname']
                dateToInsert = item['formatdate']
                
                print ("valueToInsert="+str(valueToInsert))
                print ("attributeToInsert="+str(attributeToInsert))
                print ("dateToInsert="+str(dateToInsert))
                
                with connSqlLite:
                    print("Add new data to dataset")
                    update_dataset(connSqlLite, item, element['dit_id'])
        
        
    print ("End!!!")
    
 
if __name__ == '__main__':
    main()
