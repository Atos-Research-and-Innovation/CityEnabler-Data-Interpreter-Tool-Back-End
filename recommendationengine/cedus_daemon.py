import requests
import json
import urllib
import datetime
import urllib.request
from urllib.error import HTTPError
import pandas as pd
import os

import logging
import logging.handlers
logging.basicConfig(
    format = '%(asctime)s %(levelname)s - \t%(message)s',
)
log = logging.getLogger("main")
log.setLevel(logging.DEBUG)

log.addHandler(logging.handlers.RotatingFileHandler('daemon_log.log',maxBytes=10485760,backupCount=10))
 
#check if a file (CSV) has a date cells 
def validateFileByURL_v1(urlrecovered, formatfilein):
    print ("dins validateFileByURL_v1")
    #print (urlrecovered) 
    
    try:
        
        arraySeparadores = []
        arraySeparadores.append(',')
        arraySeparadores.append(';')
        arraySeparadores.append('\t')
        
            
        if (formatfilein=='CSV'):        
            #dataPanda = pd.read_csv(urlrecovered, header=None, encoding='utf-8', low_memory=False, error_bad_lines=False)
            
            for sep in arraySeparadores:   
                print("sep="+sep)                 
                dataPanda = pd.read_csv(urlrecovered, header=None, encoding='utf-8', low_memory=False, error_bad_lines=False,  sep=sep)
                    
                if (len(dataPanda.columns)>1):
                    break
                    
        else:
            dataPanda = pd.read_excel(urlrecovered)
            
        #print (dataPanda.keys())
        timeSeriesFile = False
        existADateinColumn = False
        
        for row in dataPanda:
            timeSeriesColumn = False
            
            #print ("dataPanda[row].values")
            #print (str(len(dataPanda[row].values)))
            cntValidDatesPerColumn = 0
            for valueInArray in dataPanda[row].values:
                #print (valueInArray)
                
                for dateformat in arrayFormatDates:
                    #if date is in other format we neet to set cnt to 0
                    cntValidDatesPerColumn = 0
                    if (validateDate(valueInArray, dateformat)):
                        #print ("is a valid data!!!!!!!!! --value="+str(valueInArray)+"--dateformat="+str(dateformat))
                        #print(valueInArray);
                        existADateinColumn = True
                        cntValidDatesPerColumn = cntValidDatesPerColumn + 1
                        #print ("cntValidDatesPerColumn="+str(cntValidDatesPerColumn))
                        break
                
                if (cntValidDatesPerColumn>=(len(dataPanda[row].values)-1)):
                   timeSeriesColumn = True
                   timeSeriesFile = True
                   break
        
        if (timeSeriesFile):
            print ("Valid dataset by column")
            validDataset = True
        #else:
        elif (existADateinColumn):
            #if any cell of the columns checked before has a date is not necessary to check rows
            #check rows
            validDataset = False
            #print (dataPanda[row])
        
            #print("Total rows: {0}".format(len(dataPanda)))
            timeSeriesRow = False
            for index, row in dataPanda.iterrows():
                #print (index)
                #print (row.values)
    
                cntValidDatesPerRow = 0
                for valueInArray in row.values:
                    #print (valueInArray)
                    
                    for dateformat in arrayFormatDates:
                        #if date is in other format we neet to set cnt to 0
                        cntValidDatesPerColumn = 0
                        if (validateDate(valueInArray, dateformat)):
                            #print ("is a valid data!!!!!!!!!")
                            #print(valueInArray);
                            cntValidDatesPerRow = cntValidDatesPerRow + 1
                            #print ("cntValidDatesPerColumn="+str(cntValidDatesPerColumn))
                            break
                    
                    #print("cntValidDatesPerRow="+str(cntValidDatesPerRow))
                    if (cntValidDatesPerRow>=(len(row.values)-1)):
                       timeSeriesRow = True
                       timeSeriesFile = True
                       break
            
            if (timeSeriesFile):
                print ("Valid dataset by row")
                validDataset = True
            else:
                validDataset = False

        else:
            validDataset = False
        
        
                       
                                                       
        if (validDataset):
            print ("valid dataseet")
        else:
            print ("NO valid dataseet")
        
        return validDataset
    
    
   
    except:
        print ("---->Error in validateFileByURL_v1 !!!!!!!!")
        #print (ValueError)
        return False   


def validateFileByURL_v2(urlrecovered, formatfilein):
    print ("dins validateFileByURL_v2")
    #print (urlrecovered) 
    print ("url")    
    print (urlrecovered)
    

        
    try:
   
        arraySeparadores = []
        arraySeparadores.append(',')
        arraySeparadores.append(';')
        arraySeparadores.append('\t')

        
            
        if (formatfilein=='CSV'):        
            #dataPanda = pd.read_csv(urlrecovered, header=None, encoding='utf-8', low_memory=False, error_bad_lines=False)
            
            print ("url")
            print (urlrecovered)
            #urlrecovered = 'http://www.statweb.provincia.tn.it/indicatoristrutturali/exp.aspx?fmt=csv&idind=338&t=i'

            
            for sep in arraySeparadores:   
                print("...sep="+sep)                 
                dataPanda = pd.read_csv(urlrecovered, header=None, encoding='utf-8', low_memory=False, error_bad_lines=False,  sep=sep)
                    
                if (len(dataPanda.columns)>1):
                    break
                    
        else:
            dataPanda = pd.read_excel(urlrecovered)
            
        #print (dataPanda.keys())
        timeSeriesFile = False
        existADateinColumn = False
        
        
        for column in dataPanda:
            timeSeriesColumn = False
            #test = pd.to_datetime(dataPanda[row], errors ='coerce')
            try:

                print("------------------")
                print("----------column--------")
                print("------------------")
                print("------------------")
                print("column="+str(column))
                print (dataPanda[column])
                print("------------------")
            
                
                dataPandaToUpdate = dataPanda
                #replace header of the row to avoid problems wit the time conversion
                dataPandaToUpdate[column][0]=""
                #replace null by random string to avoid to transform null in time                
                dataPandaToUpdate[column].fillna('lolol', inplace=True)
                
                print ("dataPandaToUpdate")
                print (dataPandaToUpdate[column])
                
                test_column = pd.to_datetime(dataPandaToUpdate[column])
                print ("test_column")
                print(test_column)
                
                timeSeriesFile = True
                
                print("Time series by column!!")
                break
            
            except:
                print("not time series by columnn")
                
        validDataset = False    
        #check rows
        if (timeSeriesFile):
            print ("Valid dataset by column")
            validDataset = True
        else:
            for index, row in dataPanda.iterrows():
                try:
                    print ("---------row index="+str(index)) 
                    print (row)
                    row[0]=''
                    row[0]= fillna('lolol', inplace=True)
                    print (row)
                    test_row = pd.to_datetime(row)
                    print ("test_column")
                    print(test_column)
                    print("Time series by row!!")
                    
                    validDataset = True
                    
                    break
                except:
                    print("not time series by row")
            
        
        
                       
                                                       
        if (validDataset):
            print ("valid dataseet")
        else:
            print ("NO valid dataseet")

        
        return validDataset
    
    

   
    except:
        print ("---->Error in validateFileByURL_v2 !!!!!!!!")
        
        #print (ValueError)
        return False 
        
#validate if a strings follows a date patterm
def validateDate(date_text, formatDateIn):
    #print (date_text)
    try:
        #datetime.datetime.strptime(date_text, '%Y-%m-%d')
        datetime.datetime.strptime(str(date_text), formatDateIn)
        return True
    except ValueError:
        #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        return False        

#post data to the CDW
def CedusPostTimSeries(apiBase, data, dataToSend):
    print ("Start CedusPostTimSeries")  
    
    print("apiBase")
    print(apiBase)
    post_items_as_timeseries = data["post_items_as_timeseries"]
    
    log.info("[INFO] Start CedusPostTimSeries")
    log.info("[INFO] post_items_as_timeseries: "+str(post_items_as_timeseries))
    
    print("dataToSend")
    print(dataToSend)
    
    log.debug("[DEBUG] dataToSend")
    log.debug(dataToSend)
    
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    #r = requests.post(post_items_as_timeseries, data=json.dumps(dataToSend),  headers=headers)  

    username = data['username']
    password = data['password']
    
    r = requests.post(post_items_as_timeseries, data=json.dumps(dataToSend),  headers=headers, auth=requests.auth.HTTPBasicAuth(username, password))

    print ("r.status_code")
    print (r.status_code)
    log.info("[INFO] r.status_code: "+str(r.status_code))
    
    if r.status_code == 200:
        print ("Response POST time series OK")
        log.info("[INFO] Response POST time series OK")
    else:
        print ("Response get nodes KO")            
        log.error("[ERROR] Response POST time series KO")
        log.error(r)
    
    print ("r")
    print (r)
        
    print ("End CedusPostTimSeries")
    return r.status_code

#get the list of nodes availables in the CDW    
def CedusSearchNodesProxy(apiBase, data):
    print ("CedusSearchNodesProxy")

    log.info("[INFO] ------------------")
    log.info("[INFO] CedusSearchNodesProxy")
     
            
    get_nodes_api = data["get_nodes_api"]
      
    print("get_nodes_api " + get_nodes_api)
    
    log.info("[INFO] get_nodes_api " + get_nodes_api)
    
    dataToSend = {}
    #search for nodes
    
    
    if get_nodes_api is None:
        print ("Error get nodes api URL is missing")
        log.error("[ERROR] Error get nodes api URL is missing")
    else:
        # r = requests.get(apiBase+'/FederationManager/resources/services/nodes')
        r = requests.get(get_nodes_api)
        

        print ("r.status_code")
        print (r.status_code)
        
        log.info("[INFO] Return code:"+str(r.status_code))
          
        #print ("r.json")
        #print (r.json)
        
        if r.status_code == 200:
            print ("Response get nodes OK")
            log.info("[INFO] Response get nodes OK")
            dataToSend = r.json();
        else:
            print ("Response get nodes KO")            
            print (r)
            
            log.error("[ERROR] Response get nodes OK")
            log.error(r)


    return dataToSend


#get the number of items that fullfill the search conditions in the CDW
def CedusCountProxy(apiBase, data, nodesIn, term, start, dateFiltered, filterBy):
    
    
    get_count = data["get_count"]    
    print("get_count " + get_count)
    
    print ("CedusCountProxy search by "+str(get_count))
    
    log.info("[INFO] ------------------")
    log.info("[INFO] CedusCountProxy search by "+str(get_count))
    
        
    
    
    
    print (filterBy)
    
    today = datetime.date.today() 
    #print("APIBASE " + apiBase)
    #print("nodes " + nodesIn)
    #print("term " + term)
    #print("start " + str(start))
    
    dataToSend = {}
    
    if start is None:
        start = "0"
    
    
    start = str(start);
    nodes = nodesIn.split(',')
    
    #if apiBase is None or term is None:
    if get_count is None or term is None:    
        print ("Error get_count is missing")
    
    else:
        headers = {'content-type': 'application/json'}
        
        print ("dateFiltered")
        print (dateFiltered)
        
        if (dateFiltered=="" or dateFiltered=="never"):
            print ("**********NO filter by date")
            #filter by modified date
            #dataToPost = json.dumps({"parameter":[{"filter":"ALL","text":term}, {"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes})

            dataToPost = json.dumps({"filters": [{"field": "ALL","value": term}],"live": False,"sort": {"field": "title","mode": "asc"},"rows": "5","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
            
            
            
                                   
        else:
            print ("filter by date")
            #filter by modified date

            #dataToPost = json.dumps({"parameter":[{"filter":"ALL","text":term}, {filterBy:{"start":str(dateFiltered),"end": str(today)}}, {"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes})
            
            dataToPost = json.dumps({"filters": [{"field": "ALL","value": term}], filterBy: {"start": str(dateFiltered)+"T00:00:00Z", "end": str(today)+"T00:00:00Z"}, "live": False,"sort": {"field": "title","mode": "asc"},"rows": "5","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
            

        
        print ("------------------")
        print (dataToPost)
        print ("------------------")
        
        r = requests.post(get_count, data= dataToPost, headers=headers)
    
        print ("datatopost")
        print (dataToPost)        
        print (r.status_code) 

        print ("-----r.json() --------count:")
        print (r.json());

        if r.status_code == 200:
            print ("search cnt OK")
            dataToSend = r.json()
            #return Response(r.json(), status=status.HTTP_200_OK)
        else:
            print ("search cnt KO")
      
            
    return dataToSend


#get the data acording search conditions CDW
def CedusSearchProxy(apiBase, data, nodesIn, term, start, dateFiltered, filterBy):

    print ("CedusSearchProxy search by ")
    get_search = data["get_search"]    
    print("get_search " + get_search)
    print("filterBy " + filterBy)

    
    log.info("[INFO] ------------------")
    log.info("[INFO] CedusSearchProxy search by ")
    log.info("[INFO] get_search " + get_search)
    log.info("[INFO] filterBy " + filterBy)    
    
    
    #print("APIBASE " + apiBase)
    #print("nodes " + nodesIn)
    #print("term " + term)
    #print("start " + str(start))

    dataToSend = {}
    
    if start is None:
        start = "0"
    
    
    start = str(start);
    nodes = nodesIn.split(',')
    

    #if apiBase is None or term is None:
    if get_search is None or term is None:
        print ("Error apibase is missing")
    
    else:    
        headers = {'content-type': 'application/json'}

        if (dateFiltered=="" or dateFiltered=="never"):
            print ("no filter by date")
            #dataToPost = json.dumps({"parameter":[{"filter":"ALL","text":term}, {"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes})
            
            
            dataToPost = json.dumps({"filters": [{"field": "ALL","value": term}],"live": False,"sort": {"field": "title","mode": "asc"},"rows": "5","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
                                    
        else:
             
            #dataToPost = json.dumps({"parameter":[{"filter":"ALL","text":term},{filterBy:{"start":str(dateFiltered),"end": str(today)}}, {"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes})
            
            dataToPost = json.dumps({"filters": [{"field": "ALL","value": term}], filterBy: {"start": str(dateFiltered)+"T00:00:00Z", "end": str(today)+"T00:00:00Z"}, "live": False,"sort": {"field": "title","mode": "asc"},"rows": "5","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
        
        

        print ("------------------>dataToPost----------->")
        print (dataToPost)

        log.debug("[DEBUG] dataToPost:")
        log.debug(dataToPost)
         

        
        r = requests.post(get_search, data= dataToPost, headers=headers)
        
        print ("r.status_code------------")
        print (r.status_code)
        print ("--------------")
        print (r)
        print ("--------------")

        log.debug("[DEBUG] r.status_code:"+str(r.status_code))
        log.debug(r)
        
        if r.status_code == 200:
            print ("search list OK")
            log.info("[INFO] search list KO")
            dataToSend = r.json()
            #return Response(r.json(), status=status.HTTP_200_OK)
        else:
            print ("search list KO")
            log.error("[ERROR] search list KO")
            log.error(r)


    return dataToSend


if __name__ == '__main__':

    #filename = "output.txt"
    filename = "output_time_series_in_CDW.csv"
    #logfilename = "daemon.log"
    datasetsAsTimeSeries = []
    
    '''
    with open(logfilename, 'a') as out:
        out.write('[INFO] Start at '+str(datetime.datetime.now())+' \n') 
    '''

    log.info("[INFO] ** Start at "+str(datetime.datetime.now()))

    #log.error("ERROR") 
    #log.debug("DEBUG")
    #log.info("INFO")
    #log.critical("CRITICAL")
        
    with open('config.json') as data_file:
        print ("opening config file...")     
        data = json.load(data_file)
        
    #apiBase = 'http://dataworkspace.eng.it/'
    apiBase = data["apiBase"]    
    
    wordToSearch = ''
    lastexecution = "never"
    
    #wordToSearch = 'trento'
    #wordToSearch = 'polen'
    #wordToSearch = 'malaga'
    wordToSearch = data["wordToSearch"]
    
    
    filterbylastexecution = data["filterbylastexecution"]
    
    
    if (filterbylastexecution):
        lastexecution = str(data["lastexecution"])


    #wordToSearch = 'Imprese 10'
    #wordToSearch = 'Campanillas'
    #wordToSearch = 'Imprese'
    wordToSearch = 'menores'
    lastexecution = "never"
        
    today = datetime.date.today()
    startTime = today
    
    print ("------------------")
    print ("Get List of Nodes")
    print ("time ->"+str(startTime)+"<----")
    print ("word to search ->"+str(wordToSearch)+"<----")
    print ("filterbylastexecution ->"+str(filterbylastexecution)+"<----")
    print ("lastexecution ->"+str(lastexecution)+"<----")
    print ("------------------")
    

    log.info("[INFO] ------------------")
    log.info("[INFO] ------------------ Get List of Nodes --------------")
    log.info("[INFO] time ->"+str(startTime)+"<---- ")
    log.info("[INFO] word to search ->"+str(wordToSearch)+"<---- ")
    log.info("[INFO] filterbylastexecution ->"+str(filterbylastexecution)+"<---- ")
    log.info("[INFO] lastexecution ->"+str(lastexecution)+"<---- ")
    log.info("[INFO] ------------------ ")
    
    
    nodesList = CedusSearchNodesProxy(apiBase, data);
    print (nodesList);
    
    log.debug ("[DEBUG] ------------------ ")
    log.debug ("[DEBUG] nodesList: ")
    log.debug (json.dumps(nodesList))
    log.debug ("[DEBUG] ------------------ ")
    
    arrayFormatDates = []
    arrayFormatDates.append('%Y-%m-%d')
    arrayFormatDates.append('%Y-%d-%m')
    arrayFormatDates.append('%Y%m%d')
    arrayFormatDates.append('%Y%d%m')
    
    arrayFormatDates.append('%m-%d-%Y')
    arrayFormatDates.append('%d-%m-%Y')    
    arrayFormatDates.append('%d%m%Y')
    arrayFormatDates.append('%m%d%Y')

   
    
    #delete log file if exists
    '''
    try:
        os.remove(filename)
    except OSError:
        pass
    '''
   
    with open(filename, 'a') as out:
        #out.write('Start!!! \n')    
        out.write('Id;Title;URL \n')
    
   
    cntLoop=1 #this is the initial variable    
    if (lastexecution=="" or lastexecution=="never"):
        #we recover all
        cnt = 2
        cntLoop=2
    else:
        #we recover items by update and create date
        cnt = 1
        cntLoop=2
    
    print ("cntLoop")
    print (cntLoop)

    log.info("[INFO] cntLoop="+str(cntLoop))
    log.info("[INFO] cnt="+str(cnt))


    cntTimeSeries = 0;    
    cntTotalItemsRec = 0;
    
    while cnt <= cntLoop :
        #inside of while loop
        print (cnt),
        print ("This is inside of while loop")
        if (cnt==2):
            filterBy = 'modified' 
        elif (cnt==1):
            filterBy = 'issued'
            #filterBy = 'created'

        log.info("[INFO] filterBy="+str(filterBy))

        
        if (nodesList):        
            #if (nodesList['nodes']):  
        
            idsNodes = ""
              
            #for nodedetail in nodesList['nodes']:
            for nodedetail in nodesList:
                #print (nodedetail['id'])
                #print (nodedetail['nodeState'])
                #if (nodedetail['nodeState']=='ONLINE'):
                if (1==1):
                    if (idsNodes):
                        idsNodes += ','
                    
                    idsNodes = idsNodes + str(nodedetail['id'])
            
            
            print ("------------------")
            print ("Get List of results")
            print ("------------------")
            print ("idsNodes:")
            print (idsNodes)
            
            idsNodes = "15"
            
            #print ("idsNodes:")
            #print (idsNodes)
            
            countResult = CedusCountProxy(apiBase, data, idsNodes, wordToSearch, 0, lastexecution, filterBy)

            print ("countResult------------------------")
            print (countResult)
            
            #tmpCnt = CedusSearchProxy(apiBase, data, idsNodes, wordToSearch, 0, lastexecution, filterBy)
            #countResult = {'count':tmpCnt['count']}            
            #countResult = {'count':2}
            
            
            cntotal = 0
            start = 0
        
            print("countResult")
            print(countResult)

           
            log.info("[INFO] ------------------")
            log.info("[INFO] number items found: ")
            log.info(json.dumps(countResult))
            log.info("[INFO] ------------------")
            
            #print ("len(tmpCnt['results'])")
            #print (len(tmpCnt['results']))
        
            #if (1==2):
            if (countResult):
                if (countResult['count']):
                    if (countResult['count']>0):
                    
                        print (countResult);
                        print ("countResult['count'] = "+str(countResult['count']))
                    
                        while (start < countResult['count']):
                        
                            print ("--------->start="+str(start))
                            listCedusItems = CedusSearchProxy(apiBase, data, idsNodes, wordToSearch, start, lastexecution, filterBy) 
                            
                            #start = start +1
                            
                            print ("--cnt items search:")
                            print (listCedusItems['count']);
                           
                            #if (1==2):
                            if (listCedusItems['count']>0):
                                
                                
                                for itemdetail in listCedusItems['results']:
                                    
                                    start = start + 1
                                    
                                    print ("|||-//-//-//-//-//-//-//-//->start="+str(start))
                                    
                                    cntTotalItemsRec = cntTotalItemsRec + 1
                                    #print (itemdetail['id']);
                                    cntdist = 0;
                                    cntotal = cntotal +1
                                    
                                
                                    print ("ID:"+str(itemdetail['id']))
                                    #print (itemdetail['keywords'])
                                    #print ("Tilte:"+str(itemdetail['dcat_title']['value']))
                                    #print ("cnt distributions: "+str(len(itemdetail['dcat_distributions'])))
                                    
                                    log.debug("[DEBUG] ID:"+str(itemdetail['id']))
                                    #log.debug("[DEBUG] Tilte:"+str(itemdetail['dcat_title']['value']))
                                    log.debug("[DEBUG] cnt distributions: "+str(len(itemdetail['dcat_distributions'])))
                                    
                                    if (len(itemdetail['dcat_distributions'])==0):
                                        print ("This dataset don't have dcat_distributions")
                                    
                                    #log.debug("**************")
                                    #log.debug(itemdetail)
                                    #log.debug("**************")
                                    cntDistributions = 0
                                    for distributions in itemdetail['dcat_distributions']:
                                        #if is CASV we check contenct
                                        cntDistributions = cntDistributions + 1
                                        print("----- cntDistributions="+str(cntDistributions))
                                        print (distributions['dcat_format']['value'])
                                        
                                        if ((distributions['dcat_format']['value']=='CSV') or (distributions['dcat_format']['value']=='XLS') or (distributions['dcat_format']['value']=='XLSX')):
                                            
                                            print ("---------------Distribution-------------")
                                            print (distributions['dcat_format']['value'])
                                        
                                            validDataset = False
                                            #print (distributions['dcat_format']['value']);
                                            #print (distributions['dcat_accessUrl']['value']);
                                            #itemdetail['dcat_distributions'][cntdist]['mmp'] = cntdist;
                                        
                                            #print ("cntotal="+str(cntotal))
                                        
                                            try:
                                                urlrecovered = distributions['dcat_accessUrl']['value'];
                                                format = distributions['dcat_format']['value'];
                                                #urlrecovered = 'http://datosabiertos.malaga.eu/recursos/cultura/telecentros/gp_telecentros.csv'    
                                                print("----------------------------------------------")                                                
                                                print (urlrecovered)
                                                print(distributions['dcat_format']['value'])
                                            
                                                #isValidCSV = validateFileByURL_v1(urlrecovered, distributions['dcat_format']['value'])
                                                isValidCSV = validateFileByURL_v2(urlrecovered, distributions['dcat_format']['value'])
                                                
                                                
                                                #isValidCSV = false
                                                
                                                #isValidCSV = True
                                            
                                                print ("isValidCSV----------------------")
                                                print (isValidCSV)
                                            
                                                if (isValidCSV):
                                                
                                                    cntTimeSeries = cntTimeSeries + 1
                                                
                                                    dataToSend = {"assetType": "DCATDataset",
                                                              "assetId": itemdetail['id'],
                                                              "nodeId" : itemdetail['nodeID'],
                                                              "attrType": "Boolean",
                                                              "attrName": "istimeserie",
                                                              "attrValue": "true"
                                                              }
                                                    #print ("itemdetail")
                                                    #print (itemdetail)
                                                    datasetsAsTimeSeries.append(dataToSend)
                                                
                                                    with open(filename, 'a') as out:                                                
                                                        #out.write("id="+itemdetail['id']+"<- Tite="+itemdetail['dcat_title']['value']+"<- URL="+urlrecovered + '\n')
                                                        out.write(itemdetail['id']+";"+itemdetail['dcat_title']['value']+";"+urlrecovered + '\n')
                                                    
                                                        #out.write(urlrecovered + '\n')
                                                    
                                                    #if one itemm is timeserie is enogh
                                                    
                                                    

                                                    log.info("[INFO] ------------------")
                                                    log.info("[INFO] We found timeseries, cntTimeSeries= "+str(cntTimeSeries)+" ")
                                                    log.info(json.dumps(datasetsAsTimeSeries))
                                                    log.info("[INFO] ------------------")
        
                                                    print ("******************************")
                                                    print (datasetsAsTimeSeries)
                                                    print ("******************************")
                                                    
                                                    
                                                    dataPostedCode = CedusPostTimSeries(apiBase, data, datasetsAsTimeSeries)

                                                    log.info("[INFO] ------------------")
                                                    log.info("[INFO] POST resonse" + str(dataPostedCode) +" ")
                                                    log.info("[INFO] ------------------")
                                                    
                                                    if (dataPostedCode==200):
                                                        print("POST OK")
                                                    else:
                                                        print("POST KO, code:")
                                                        print (dataPostedCode)
                                                    
                                                    
                                                    datasetsAsTimeSeries = []
                                                    
                                                    
                                                    break
                                                    
                                                    #req = urllib.request.Request(urlrecovered)
        
                                                    #urllib.request.urlopen(req)
                                                    
                                            except ValueError:
                                                print ("Some errror!!!")
                                                log.error("[ERROR] Some errror finding if is a time series!!! ") 
                                            
                                            cntdist = cntdist +1;
                            
                            else:
                                start = start +1
                                
            else:
                print ("No data Found")
                log.info("[INFO] No data Found") 

        else:
            print ("No nodes found")        
            log.info("[INFO] No nodes found")
        
        cnt+=1;
    else :
        #this statement will be printed if cnt is out of the loop
        print (cnt),
        print("This is outside of while loop")
   
    
    
    print ("cntTimeSeries")
    print (cntTimeSeries)
    
    log.info("[INFO] Number of time series found:"+str(cntTimeSeries))
    
    print ("cntTotalItemsRec")
    print (cntTotalItemsRec)
    
    print ("THE END !!!!!")


    ## Save our changes to JSON file
    data["lastexecution"]=str(startTime)
    jsonFile = open("config.json", "w+")
    jsonFile.write(json.dumps(data))
    jsonFile.close()

    '''
    with open(logfilename, 'a') as out:
        out.write('[INFO] End at '+str(datetime.datetime.now())+' \n')
    '''
    log.info("[INFO] ** End at "+str(datetime.datetime.now()))   