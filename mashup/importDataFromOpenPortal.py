#!/usr/bin/python
import sqlite3
from sqlite3 import Error
import requests
import json
import urllib
import datetime
import urllib.request
from urllib.error import HTTPError
import pandas as pd
import os

from datetime import datetime as dt
from datetime import timedelta, date

import logging
import logging.handlers


relation_data_dit = [{                         
                        'url_csv_file': "http://datosabiertos.malaga.eu/recursos/ambiente/polen/2017.csv",
                        'sep': ',',
                        'sql_database_path': "/home/miquel/PolicyCompass/policycompass/backendv2/CEDUS/pcompass.db"
                        }]


    
logging.basicConfig(
    format = '%(asctime)s %(levelname)s - \t%(message)s',
)
log = logging.getLogger("main")
log.setLevel(logging.DEBUG)

log.addHandler(logging.handlers.RotatingFileHandler('importDataFromOpenPortal.log',maxBytes=10485760,backupCount=10))

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)


#check if a value is a number
def isaNaumber(value):
    
    validValue = False
    
    try:
        float(value)
        validValue = True 
    except ValueError:
        #print(ValueError)
        validValue = False
        
    return validValue


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


#get all datasets by resource url
def datasetsByDatasource(conn, resource_url):
    cur = conn.cursor()
    #cur.execute("SELECT * FROM datasetmanager_dataset WHERE id=?", (dataset_id,))
    '''
    cur.execute("SELECT id, title FROM datasetmanager_dataset WHERE resource_url=?", (resource_url,))
    
    rows = cur.fetchall()
    '''
    rows = [{
             0:70,
             1:'tilte_dataset_70'
    },
    {
             0:71,
             1:'tilte_dataset_71'
    }]    
    
    
    return rows

#To create a new individual
def createIndividual(conn, individualName):
    
    id = 0
    cur = conn.cursor()
    #cur.execute("SELECT * FROM datasetmanager_dataset WHERE id=?", (dataset_id,))
    print ("individualName-----------------")
    print (individualName)
    cur.execute("INSERT INTO referencepool_individual (title, data_class_id, code) VALUES (?,?,?) ", (individualName,'7', ''))
    #select id, title from referencepool_individual where title='Andorra'
    conn.commit()
    
#get individual id by name
def getIndividuaIdByName(conn, individualName):
    
    id = 0
    cur = conn.cursor()
    #cur.execute("SELECT * FROM datasetmanager_dataset WHERE id=?", (dataset_id,))
    cur.execute("SELECT id, title FROM referencepool_individual WHERE title=?", (individualName,))
    #select id, title from referencepool_individual where title='Andorra'
    
    rows = cur.fetchall()
    
    for row in rows:
        id = row[0]
    
    print ("id")
    print (id)
    '''
    if (id==0):
        #print ("create indivdual")
        #print (individualName)
        createIndividual(conn, individualName)
        id = getIndividuaIdByName(conn, individualName)
        #print ("id--------------")
        #print (id)
    '''
    
    return id

   

#update data in DIT
def update_dataset(conn, item, dataset_id):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param item:
    :return:
    """
    #print("update_dataset")
    #print (item)
    #dataset_id = 73
    
    #print (str(dataset_id))
    
    cur = conn.cursor()

    #title2 = 'wwwwwwwww'
    #cur.execute("UPDATE datasetmanager_dataset SET title=? WHERE id=70", (title2,))
        
    #cur.execute("SELECT * FROM datasetmanager_dataset WHERE id=?", (dataset_id,))
    cur.execute("SELECT time_end, time_start, time_resolution, data, title FROM datasetmanager_dataset WHERE id=?", (dataset_id,))

    rows = cur.fetchall()
    for row in rows:
        #print ("title")
        #print (row[4])
        #print ("dataset time_end")
        #print(row[0])
        #print ("time input")
        dataIn = item['date']
        #print (dataIn)
        
        #print ("value input")
        valueIn = float(item['value'])
        #print (valueIn)
       
        #print ("individual input")
        ditIndividualId = item['ditIndividualId']
        #print (ditIndividualId)

        new_end_date = row[0]
        
        a = dt.strptime(row[0], "%Y-%m-%d")
        b = dt.strptime(str(dataIn), "%Y-%m-%d")
        
        
        if (b>a):
            new_end_date = dataIn
        
        #print("new_end_date")
        #print(new_end_date)               
        
        d = json.loads(row[3])
        #print("json data_frame")
        #print(d['data_frame'])
        
        tmpjson = d['data_frame']
        tmpjson = tmpjson.replace("None", 'null')
        
        #jsonDataFrame = json.loads(d['data_frame'])
        
        #print ("tmpjson---------------")
        #print (tmpjson)
        
        jsonDataFrame = json.loads(tmpjson)
        
        new_row_json = {}
        
        for dataFrame  in jsonDataFrame:
            row_id = dataFrame
            #print(row_id)
            if (str(ditIndividualId)==str(row_id)):
                #print("------------------------row_id")
                #print(row_id)       
                #print(jsonDataFrame[dataFrame])
                individualData = jsonDataFrame[dataFrame]
                updateValue = False 
                
                
                
                new_date = str(dataIn)+'T00:00:00.000Z'
                new_data = float(valueIn);

                if (new_date=='2017-06-18T00:00:00.000Z'):
                    print ("new_data")
                    print(new_data)
                    
                jsonDataFrame[dataFrame][new_date] = new_data
            
                #print ("final json updated")
                #print(jsonDataFrame[dataFrame])

                #print ("row")
                #print (row)
                
                array_start_date = str(row[1])
                array_start_date = array_start_date.split("-")
            
                #print ("array_start_date")
                #print (array_start_date)
                
                array_end_date = str(new_end_date)
                array_end_date = array_end_date.split("-")

                #print ("array_end_date")
                #print (array_end_date)
                
                #print (int(array_start_date[0]))
            
                start_date = datetime.date(int(array_start_date[0]), int(array_start_date[1]), int(array_start_date[2]))
                
                #print("start_date")
                #print(start_date)
                
                end_date = datetime.date(int(array_end_date[0]), int(array_end_date[1]), int(array_end_date[2]))

                for single_date in daterange(start_date, end_date):
                    #print (single_date.strftime("%Y-%m-%d"))  
                    new_date_to_check =  single_date.strftime("%Y-%m-%d")+'T00:00:00.000Z'
                
                
                    if (new_date_to_check in jsonDataFrame[dataFrame]):
                        #print (new_date_to_check)
                        #print("existe")
                        #print (jsonDataFrame[dataFrame][new_date_to_check])
                        if (jsonDataFrame[dataFrame][new_date_to_check]== None):
                            jsonDataFrame[dataFrame][new_date_to_check] = 'null'
                            #jsonDataFrame[dataFrame][new_date_to_check] = 55
                    else:
                        #jsonDataFrame[dataFrame][new_date_to_check] = None
                        #jsonDataFrame[dataFrame][new_date_to_check] = 0
                        jsonDataFrame[dataFrame][new_date_to_check] = 'null'
                        #jsonDataFrame[dataFrame][new_date_to_check] = 55

        
                #print (" super final json updated")
                #print(jsonDataFrame[dataFrame])    
        
                #print ("row_id")
                #print (row_id)
                
                new_row_json[row_id] = jsonDataFrame[dataFrame]        
        

            
            else:
                
                new_row_json[row_id] = jsonDataFrame[dataFrame]
        
        
        
        new_row_json_string = str(new_row_json)
    
    
        new_row_json_string = new_row_json_string.replace("'", '\\"')
                
        final_json = {"data_frame": new_row_json_string, "resolution": d['resolution'], "unit": d['unit']}
        #print("final_json")
        #print(final_json)
        final_json_as_string = str(final_json)
        final_json_as_string = final_json_as_string.replace("'", '"')
        final_json_as_string = final_json_as_string.replace("\\\\", "\\")
            

        final_json_as_string = final_json_as_string.replace('\\"null\\"', "null")

        #print("final_json_as_string")
        #print(final_json_as_string)
        
        #print("new_end_date")
        #print(new_end_date)
        #print("dataset_id")
        #print(dataset_id)
        #print("final_json_as_string")
        #print(final_json_as_string)
        
        #
        try:
            
            final_json_as_string = final_json_as_string.replace("None", 'null')
            
            cur.execute("UPDATE datasetmanager_dataset SET time_end=?, data=? WHERE id=?", (new_end_date, final_json_as_string, dataset_id,))
            conn.commit()
            
    
        except Error as e:
            print ("error update dataset")
            print ("id dataset")
            print (dataset_id)
            print(e)
                
    


if __name__ == '__main__':


    log.info("[INFO] ** Start at "+str(datetime.datetime.now()))
    

    for element in relation_data_dit:
    
        #urlrecovered = "http://datosabiertos.malaga.eu/recursos/ambiente/polen/2017.csv"
        urlrecovered = str(element['url_csv_file'])
        sep = str(element['sep'])
        
        dataPanda = pd.read_csv(urlrecovered, header=None, encoding='utf-8', low_memory=False, error_bad_lines=False,  sep=sep)
    
    
        #database = "/home/miquel/PolicyCompass/policycompass/backendv2/CEDUS/pcompass.db"
        
        database = str(element['sql_database_path'])
        
        #database = "/home/ubuntu/CEDUS/BE/pcompass.db"
        connSqlLite = create_connection_sqlite(database)  
        
        
        #select all datasets that uses this resource
        datasetsByDatasource = datasetsByDatasource(connSqlLite, urlrecovered)
        
    
    
        cnt = len(dataPanda)
        print (cnt)
        
        arrayTemporalDatos = []
        
        item = []
        for index, row in dataPanda.iterrows():
            if (index==0):
    
                cntIndexRow = 0;
                for indexR in row:
                    
                    
                    
                    #if ( ("valor" in (str(row[cntIndexRow]))) or ("previsin" in (str(row[cntIndexRow]))) ):
                    if ( ("FECHA" in (str(row[cntIndexRow]))) or ("LAT" in (str(row[cntIndexRow]))) or ("LON" in (str(row[cntIndexRow])))):
                        print("no valid")
                        print (str(row[cntIndexRow]))
                    else:
                    #if (1==1):
                        print (row[cntIndexRow])
                        individualName = row[cntIndexRow]
                        individualid= getIndividuaIdByName(connSqlLite, individualName)
                        
                        if (individualid>0):
                            #print ("individualid")
                            #print (individualid)
                            #print ("-----------------")
                            
                            arrayTemporalDatos.append({'colindex':cntIndexRow, 'name':individualName, 'ditIndividualId': individualid})
                        
                    cntIndexRow = cntIndexRow + 1            
                   
                    
            else:
                
                #print ("arrayTemporalDatos")
                #print (arrayTemporalDatos)
                
                date = row[0]
                
                year = date[0:4]
                month = date[4:6]
                day = date[6:8]
                
                date = year+"-"+month+"-"+day;
                
                #print (date);
                
                for indexR in arrayTemporalDatos:
                                    
                    pos = indexR['colindex']
                    #print ("pos")
                    #print (pos)
                    value = row[pos]
                    print ("value")
                    print (value)
                    
                    validValue = isaNaumber(value)

                    
                    if (validValue):
                        if (indexR['ditIndividualId']!=0):
                            
                            item = {'ditIndividualId':indexR['ditIndividualId'], 'date': date, 'value':value}
                                      
                        
                            for datasource in datasetsByDatasource:
                                print("--------------------")
                                print (datasource[0])
                                datasetid = datasource[0]
                        
                                update_dataset(connSqlLite, item, datasetid )
                    


                    

                    
                
            

    log.info("[INFO] ** End at "+str(datetime.datetime.now()))
