import requests
from django.core.files.uploadedfile import SimpleUploadedFile
from django.http.response import HttpResponse
from policycompass_services import permissions
from rest_framework import generics
from rest_framework import status
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.views import APIView
from .file_encoder import FileEncoder
from .serializers import *
from collections import OrderedDict
import json
from pandasdmx import Request
from django.conf import settings
import pandas as pd

from datetime import datetime as dt
from datetime import timedelta, date
import os

__author__ = 'fki'


class Base(APIView):
    def get(self, request):
        """
        :type request
        :param request:
        :return:
        """
        result = {
            "Datasets": reverse('dataset-list', request=request),
            "Converter": reverse('converter', request=request),
        }
        return Response(result)


class DatasetList(generics.ListCreateAPIView):
    model = Dataset
    serializer_class = BaseDatasetSerializer
    paginate_by = 10
    paginate_by_param = 'page_size'
    permission_classes = IsAuthenticatedOrReadOnly,

    def pre_save(self, obj):
        obj.creator_path = self.request.user.resource_path

    def post(self, request, *args, **kwargs):
        self.serializer_class = UpdateDatasetSerializer

        return super(DatasetList, self).post(request, args, kwargs)

    def get_queryset(self):
        queryset = Dataset.objects.all()
        indicator_id = self.request.GET.get('indicator_id', '')

        if indicator_id:
            queryset = queryset.filter(indicator_id=indicator_id)

        return queryset


class DatasetDetail(generics.RetrieveUpdateDestroyAPIView):
    model = Dataset
    serializer_class = DetailDatasetSerializer
    permission_classes = permissions.IsCreatorOrReadOnly,

    def put(self, request, *args, **kwargs):
        self.serializer_class = UpdateDatasetSerializer
        return super(DatasetDetail, self).put(request, args, kwargs)

    def delete(self, request, *args, **kwargs):
        id = kwargs.get('pk')
        user = request.user.resource_path
        dataset = Dataset.objects.get(id=id)

        if dataset.creator_path == user or request.user.is_admin is True:
            dataset.delete()
            return Response(status=status.HTTP_200_OK)

        elif hasattr(dataset,'organization'):
                
            userCanDelete = False
            #print ("user data")
            #print (request.user.organizations)
            
            #print ("dataset.organization")
            #print (dataset.organization)
            
            for x in request.user.organizations:
                #print ("x")
                #print (x['id'])
                if (str(x['id'])==str(dataset.organization)):
                    #print ("same org ID")
                    for roles in x['roles']:
                        #print ("roles")
                        #print (roles['name'])
                        if (roles['name']=='Seller'):
                            userCanDelete = True
                       
            if userCanDelete:
                dataset.delete()
                return Response(status=status.HTTP_200_OK)
            else:   
                return Response({'error': "User does not have the necessary permissions"}, status=status.HTTP_403_FORBIDDEN)  
            
        
        else:
            return Response({'error': "User does not have the necessary permissions"}, status=status.HTTP_403_FORBIDDEN)


class Converter(APIView):
    """
    Serves the converter resource.
    """

    def process_file(file, jsonData):
        # File has to be named file
        try:
            print ("Converter - process_file")
            encoder = FileEncoder(file, jsonData)

            # Check if the file extension is supported
            if not encoder.is_supported():
                return Response({'error': 'File Extension is not supported'},
                                status=status.HTTP_400_BAD_REQUEST)
            # Encode the file
            try:
                encoding = encoder.encode()
            except:
                print ("Error Converter - process_file")
                errorDict = {"result": 500}
                errorString = str(errorDict).replace("'", '"')
                errorJson = json.loads(errorString)
                return Response(errorJson)
            # Build the result
            result = {
                'filename': file.name,
                'filesize': file.size,
                'result': encoding
            }
            return Response(result)

        except:
            print ("ERROR DEC - Converter!!!!")
            errorDict = {"result": 500}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)

    def post(self, request, *args, **kwargs):
        """
        Processes a POST request
        """
        files = request.FILES
        jsonData = []

        if 'file' in files:
            return Converter.process_file(files['file'], jsonData)

        return Response({'error': "No Form field 'file'"},
                        status=status.HTTP_400_BAD_REQUEST)


class ConverterCedus(APIView):
    """
    Serves the converter resource.
    """



    def process_file(file, jsonData, dataPanda, format, sep, urlrecovered):
        # File has to be named file
        try:
            
            print("dataformat")
            print(format)
            
            '''
            print ("***********")
            print (dataPanda)
            '''
            
            sendEncoding = False
            
            #if (sep==","):
            #to try to avoid NaN values problem
            if (1==2):                
                sendEncoding = True
            else:
                jsonDataPanda = []
               
                
                for index, row in dataPanda.iterrows():
                    
                    
                    rowValue = []
                    #rowValue = row.values
                    
                    for valueInArray in row.values:
                        
                        if(pd.isnull(valueInArray)):
                            valueInArray = ''
                        
                        rowValue.append(valueInArray) 
                    
                    
                    #print ("rowValue--------------")
                    #print (rowValue)

                    
                    jsonDataPanda.append(rowValue)    

            encoder = FileEncoder(file, jsonData)
            #encoder = FileEncoder(file, jsonDataPanda)
            
            # Check if the file extension is supported
            if not encoder.is_supported():
                return Response({'error': 'File Extension is not supported'},
                                status=status.HTTP_400_BAD_REQUEST)
            # Encode the file
            try:
                encoding = encoder.encode()
            except:
                print ("ERROR DEC - ConverterCedus 2!!!!")
                errorDict = {"result": 500}
                errorString = str(errorDict).replace("'", '"')
                errorJson = json.loads(errorString)
                return Response(errorJson)
            # Build the result
            
            resultToSend = '';
            if (sendEncoding):
                resultToSend = encoding
            else:
                resultToSend = jsonDataPanda
                
            result = {
                'filename': file.name,
                'filesize': file.size,
                "result": resultToSend
            }
            return Response(result)

        except:
            print ("ERROR DEC - ConverterCedus 1!!!!")
            errorDict = {"result": 500}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)

    def post(self, request, *args, **kwargs):
        """
        Processes a POST request
        """
        files = request.FILES
        jsonData = []

        if 'file' in files:
            return Converter.process_file(files['file'], jsonData)

        return Response({'error': "No Form field 'file'"},
                        status=status.HTTP_400_BAD_REQUEST)

class EurostatSearchStep1Proxy(APIView):
    def get(self, request, *args, **kwargs):
        start = request.GET.get('start')
        term = request.GET.get('q')

        if start is None:
            start = "0"

        if term is None:
            return Response({'error': 'Invalid parameters.'},
                            status=status.HTTP_400_BAD_REQUEST)

        estat = Request("ESTAT")

        dataflows = estat.get(resource_type='dataflow').msg.dataflows

        searchResults = dataflows.find(term)

        searchResultsKeys = list(dataflows.find(term))

        resultList = []

        for key in searchResultsKeys:
            resultList.append([key, searchResults[key].name["en"]])

        results = {"result": resultList}

        return Response(results)


class EurostatSearchStep2Proxy(APIView):
    def get(self, request, *args, **kwargs):

        term = request.GET.get('q')

        if term is None:
            return Response({'error': 'Invalid parameters.'},
                            status=status.HTTP_400_BAD_REQUEST)

        estat = Request("ESTAT")

        dataflow = estat.get(resource_type='dataflow')

        dataflows = dataflow.msg.dataflows

        dsd_id = dataflows[term].structure.id

        dsd_resp = estat.get(resource_type='datastructure', resource_id=dsd_id)

        dsd = dsd_resp.msg.datastructures[dsd_id]

        dimensionsList = list(dsd.dimensions)

        dimensionsWithValues = {}

        for f in range(0, len(dimensionsList)):
            if(dimensionsList[f] != "TIME_PERIOD"):
                dimensionsValuesList = list(dsd.dimensions[dimensionsList[f]].local_repr.enum.values())
                valuesList = []
                for value in range(0, len(dimensionsValuesList)):
                    valuesList.append([dimensionsValuesList[value].id, dimensionsValuesList[value].name.en])
                dimensionsWithValues.update({dimensionsList[f]: valuesList})
            elif(dimensionsList[f] == "TIME_PERIOD"):
                print("TIME ", dsd.dimensions[dimensionsList[f]].concept.name)

        filters = {"result": dimensionsWithValues}
        filtersString = str(filters).replace("{'", '{"')
        filtersString = str(filtersString).replace("':", '":')
        filtersString = str(filtersString).replace("['", '["')
        filtersString = str(filtersString).replace("]'", ']"')
        filtersString = str(filtersString).replace("']", '"]')
        filtersString = str(filtersString).replace("',", '",')
        filtersString = str(filtersString).replace(", '", ', "')

        filterJson = json.loads(filtersString)

        return Response(filterJson)


class EurostatDownloadProxy(APIView):
    def get(self, request, *args, **kwargs):
        dataset = request.GET.get('dataset')
        filtersString = request.GET.get('filters')
        filters = {}
        query = " "

        if filtersString is None:
            filtersString = {'result': ''}

        else:
            filters = json.loads(filtersString)

        filters = filters.get('result')
        if(filters):
            for key in range(0, len(filters)):
                for value in range(0, len(filters[key][1])):
                    query += '&' + filters[key][0].lower() + '=' + filters[key][1][value]

        data = requests.get("http://ec.europa.eu/eurostat/wdds/rest/data/v2.1/json/en/" + str(dataset) + "?precision=1" + query.strip())

        if data.status_code == 400:
            errorDict = {"result": 400}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)
        elif data.status_code == 416:
            errorDict = {"result": 416}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)

        array = data.json(object_pairs_hook=OrderedDict)

        rowHeaders = array['dimension']['geo']['category']['label']

        rowHeadersValues = list(rowHeaders.values())

        rowLen = len(rowHeadersValues)

        colHeaders = array['dimension']['time']['category']['label']

        colHeadersVals = list(colHeaders.values())

        colLen = len(colHeadersVals) + 1

        colHeadersValues = []

        colHeadersValues.append("")

        for q in range(0, colLen - 1):
            colHeadersValues.append(colHeadersVals[q])

        val = array['value']

        values = list(val.values())

        rowArrays = []

        index = 0
        for row in range(0, rowLen):
            colArray = []
            position = row * (colLen - 1)
            colArray.append(rowHeadersValues[row])
            for col in range(0, colLen - 1):
                if val.get(str(position + col)) is None:
                    colArray.append("")
                else:
                    colArray.append(values[index])
                    index += 1

            rowArrays.append(colArray)

        resultArray = []
        resultArray.append(colHeadersValues)

        for y in range(0, len(rowArrays)):
            resultArray.append(rowArrays[y])

        result = {
            'filename': 'file.xls',
            'filesize': 500000,
            'result': resultArray
        }

        return Response(result)


class CKANSearchProxy(APIView):
    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def get(self, request, *args, **kwargs):
        apiBase = request.GET.get('api')
        print("APIBASE " + apiBase)
        print("q " + request.GET.get('q'))
        term = request.GET.get('q')
        start = request.GET.get('start')
        if start is None:
            start = "0"

        if apiBase is None or term is None:
            return Response({'error': 'Invalid parameters.'},
                            status=status.HTTP_400_BAD_REQUEST)

        # FIXME remove Auth
        r = requests.get(
            "%s/action/package_search?start=%s&q=%s&fq=%%28res_format:CSV%%20OR%%20res_format:TSV%%20OR%%20res_format:XLS%%20OR%%20res_format:XLSX%%29" %
            (apiBase, start, term))

        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        else:
            return Response({'error': 'Server error. Check the logs.'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CKANDownloadProxy(APIView):
    def get(self, request, *args, **kwargs):
        try:
            apiBase = request.GET.get('api')
            resourceId = request.GET.get('id')
            doConvert = request.GET.get('convert')

            if apiBase is None or resourceId is None:
                return Response({'error': 'Invalid parameters.'},
                                status=status.HTTP_400_BAD_REQUEST)

            # FIXME remove Auth
            r = requests.get(
                "%s/action/resource_show?id=%s" % (apiBase, resourceId),
                auth=('odportal', 'odp0rt4l$12'))

            if r.status_code is not 200:
                errorDict = {"result": 500}
                errorString = str(errorDict).replace("'", '"')
                errorJson = json.loads(errorString)
                return Response(errorJson)

            jsonData = r.json()

            data = requests.get(jsonData['result']['url'])

            if data.status_code is not 200:
                errorDict = {"result": 500}
                errorString = str(errorDict).replace("'", '"')
                errorJson = json.loads(errorString)
                return Response(errorJson)

            if doConvert is None:
                return HttpResponse(data.content,
                                    content_type='application/octet-stream',
                                    status=status.HTTP_200_OK)

            file = SimpleUploadedFile(
                name='file.%s' % (jsonData['result']['format'].lower()),
                content=data.content,
                content_type='application/octet+stream')

            return Converter.process_file(file, jsonData)

        except:
            errorDict = {"result": 500}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)



class CedusGetNodestProxy(APIView):
    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def get(self, request, *args, **kwargs):
        apiBase = request.GET.get('api')
        print("APIBASE " + apiBase)
        
        if apiBase is None:
            return Response({'error': 'Invalid parameters.'},
                            status=status.HTTP_400_BAD_REQUEST)
                       
        # r = requests.get(apiBase+'/FederationManager/resources/services/nodes')
        
        print ("apiBase")
        print (apiBase)
        
        if (apiBase.endswith('/')):        
            #r = requests.get(apiBase+'FederationManager/resources/services/nodes')
            r = requests.get(apiBase+'FederationManager/api/v1/administration/nodes')
        else:
            #r = requests.get(apiBase+'/FederationManager/resources/services/nodes')
            r = requests.get(apiBase+'/FederationManager/api/v1/administration/nodes')
        
        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        else:
            return Response({'error': r.status_code},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
class CedusSearchProxy(APIView):
    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def get(self, request, *args, **kwargs):
        apiBase = request.GET.get('api')
        print("APIBASE " + apiBase)
        
        term = '';
        if request.method == 'GET' and 'q' in request.GET:
            print("q " + request.GET.get('q'))
            term = request.GET.get('q')

        start = request.GET.get('start')
        nodesURL = request.GET.get('nodes')
        # nodes = []
        # nodes.append(3)
        # nodes.append(4)
        # nodes = nodesURL.split(',', 1 )
        nodes = nodesURL.split(',')
        
        print("nodes " + request.GET.get('nodes'))
        
        if start is None:
            start = "0"

        if apiBase is None or term is None:
            return Response({'error': 'Invalid parameters.'},
                            status=status.HTTP_400_BAD_REQUEST)
       
        headers = {'content-type': 'application/json'}
                
        # r = requests.post('http://217.172.12.205:8080/FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":[3,4]}), headers=headers)
        # r = requests.post('http://cityenabler.eng.it/FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":[3,4]}), headers=headers)
        # r = requests.post(apiBase+'/FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes}), headers=headers)
        
        print ("apiBase")
        print (apiBase)
        
        #dataToPost = json.dumps({"filters": [{"field": "ALL","value": term}],"live": False,"sort": {"field": "title","mode": "asc"},"rows": "10","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
        dataToPost = json.dumps({"filters": [{"field": "istimeserie","value": "true"}, {"field": "ALL","value": term}],"live": False,"sort": {"field": "title","mode": "asc"},"rows": "10","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
        
        if (apiBase.endswith('/')):        
            #r = requests.post(apiBase+'FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes}), headers=headers)
            r = requests.post(apiBase+'FederationManager/api/v1/client/search', dataToPost, headers=headers)
        else:
            #r = requests.post(apiBase+'/FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes}), headers=headers)
            r = requests.post(apiBase+'/FederationManager/api/v1/client/search', dataToPost, headers=headers)
        
        print ("r")        
        print (r)
        
        
        print (r.json)
        
        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        else:
            return Response({'url':apiBase,'error': r.status_code},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CedusCountProxy(APIView):
    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def get(self, request, *args, **kwargs):
        apiBase = request.GET.get('api')
        print("APIBASE " + apiBase)
        term = '';
        if request.method == 'GET' and 'q' in request.GET:
            print("q " + request.GET.get('q'))
            term = request.GET.get('q')
            
        start = request.GET.get('start')
        nodesURL = request.GET.get('nodes')
        # nodes = []
        # nodes.append(3)
        # nodes.append(4)
        # nodes = nodesURL.split(',', 1 )
        nodes = nodesURL.split(',')
        
        if start is None:
            start = "0"

        if apiBase is None or term is None:
            return Response({'error': 'Invalid parameters.'},
                            status=status.HTTP_400_BAD_REQUEST)
       
        headers = {'content-type': 'application/json'}
        
        # http://cityenabler.eng.it/FederationManager/resources/services/countDataset
         
        # r = requests.post('http://217.172.12.205:8080/FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":[3,4]}), headers=headers)
        # r = requests.post('http://cityenabler.eng.it/FederationManager/resources/services/search', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":[3,4]}), headers=headers)
        # r = requests.post(apiBase+'/FederationManager/resources/services/countDataset', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes}), headers=headers)
        #apiBase = 'http://217.172.12.246:8081/'
        print ("apiBase")
        print (apiBase)
        
        dataToPost = json.dumps({"filters": [{"field": "istimeserie","value": "true"}, {"field": "ALL","value": term}],"live": False,"sort": {"field": "title","mode": "asc"},"rows": "10","start": start,"nodes": nodes,"eurovocFilter": {"euroVoc": False,"sourceLanguage": "EN","targetLanguages": ["EN"]}})
        
        if (apiBase.endswith('/')):        
            #r = requests.post(apiBase+'FederationManager/resources/services/countDataset', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes}), headers=headers)
            r = requests.post(apiBase+'FederationManager/api/v1/client/countDataset', data= dataToPost, headers=headers)
        else:
            #r = requests.post(apiBase+'/FederationManager/resources/services/countDataset', data=json.dumps({"parameter":[{"filter":"ALL","text":term},{"filter":"rows","text":"10"},{"filter":"start","text":start}],"live":False,"order":{"dcat_field":"title","mode":"asc"},"nodes":nodes}), headers=headers)
            r = requests.post(apiBase+'/FederationManager/api/v1/client/countDataset', dataToPost, headers=headers)
            
        #print ("result")
        #print (r)
        
        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        else:
            return Response({'error': r.status_code},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CedusDownloadProxy(APIView):
    def get(self, request, *args, **kwargs):
        try:
            apiBase = request.GET.get('api')
            format = request.GET.get('dataformat')
            urlrecovered = request.GET.get('url')
            resourceId = request.GET.get('id')
            doConvert = request.GET.get('convert')
            
            
            # if apiBase is None or resourceId is None:
            #    return Response({'error': 'Invalid parameters.'},
            #                    status=status.HTTP_400_BAD_REQUEST)

            # data = requests.get('http://dati.trentino.it//dataset/327c1f08-2a5a-44f3-92c5-6aa033f80d36/resource/4332e7f4-2594-4081-83e2-49479b68c883/download/uscite2013qualsiasimotivazionequalsiasicontrattoanchedetestag.csv')
            data = requests.get(urlrecovered)

            arraySeparadores = []
            arraySeparadores.append(',')
            arraySeparadores.append(';')
            arraySeparadores.append('\t')            
            
            
            
            if (format=='CSV'):
                
                for sep in arraySeparadores:   
                    print("sep="+sep)         

                    
                    dataPanda = pd.read_csv(urlrecovered, header=None, encoding='utf-8', low_memory=False, error_bad_lines=False,  sep=sep)

                    print ("len with this sep")
                    print (len(dataPanda.columns))
                                        
                    if (len(dataPanda.columns)>1):
                        break
                    
            else:
                sep = ','
                dataPanda = pd.read_excel(urlrecovered)
            
            
            #print ("***********")
            #print (dataPanda)
            
            # jsonData = data.json()
            
            if data.status_code is not 200:
                errorDict = {"result": 500}                
                errorString = str(errorDict).replace("'", '"')
                errorJson = json.loads(errorString)
                return Response(errorJson)
            
            jsonData = {}
            jsonData = {'result':{'url':urlrecovered}}
            #jsonData.append({'result':urltoadd})
            
            #print("jsonData")
            #print(jsonData)
            
            
            content_type = 'application/octet-stream'
            # content_type = 'application/vnd.ms-excel'
            # content_type = 'application/vnd.msexcel'
            
            if doConvert is None:
                return HttpResponse(data.content,
                                    content_type= content_type,
                                    status=status.HTTP_200_OK)

            file = SimpleUploadedFile(
                # name='file.%s' % (jsonData['result']['format'].lower()),
                # name='file.%s' % ('tsv'),
                name='file.%s' % (format.lower()),
                content=data.content,
                content_type= content_type)

            return ConverterCedus.process_file(file, jsonData, dataPanda, format, sep, urlrecovered)
            


        except:
            # errorDict = {"result": 500}
            errorDict = {"result": 500}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)


class CedusOrganization(APIView):
    def get(self, request, *args, **kwargs):
        try:
            
            
            IDM_DEFAULT_USERNAME = settings.PC_SERVICES['IDM']['DEFAULT_USERNAME']           
            IDM_DEFAULT_PASSWORD = settings.PC_SERVICES['IDM']['DEFAULT_PASSWORD']
            
            dataToGetToken = { "auth": {"identity": {"methods": ["password"],"password": {"user": {"name": IDM_DEFAULT_USERNAME,"domain": { "id": "default" },"password": IDM_DEFAULT_PASSWORD}}}}}
                        
            url = settings.PC_SERVICES['IDM']['URL_TOKEN']
            
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            r = requests.post(url, data=json.dumps(dataToGetToken), headers=headers)
            
            rjson = r.json()
            
            #organizationId = 'f4b8ae76489f4a7b82c72bf86dade328'
            organizationId = request.GET.get('id')
            urlOrganization = settings.PC_SERVICES['IDM']['URL_ORGANIZATION']+"/"+organizationId
            #urlOrganization = 'http://cityenabler.eng.it:5000/v3/OS-SCIM/v2/Organizations/'+organizationId
            
            
            headers = {}
            headers = {'Content-Type': 'application/json', 'X-Auth-Token': r.headers['X-Subject-Token']}
            
            #headers = {'X-Auth-Token': rjson['token']['catalog'][0]['id'] }
            r = requests.get(urlOrganization, headers=headers)
            
            #rjson = r.json()
            #print("rjson 2")
            #print(rjson)
            
            return Response(r.json(), status=status.HTTP_200_OK) 
            #return Response(rjson['token']['catalog'][0]['id'], status=status.HTTP_200_OK)

        except:
            # errorDict = {"result": 500}
            errorDict = {"result": 500}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorJson)        
        
class CedusGetUserData(APIView):
    def get(self, request, *args, **kwargs):
        try:
            url = request.GET.get('url')
            access_token = request.GET.get('access_token')            
            #endurl = 'http://cityenabler.eng.it/idm/user?access_token='+access_token
            endurl = url+'?access_token='+access_token
            
            headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
            r = requests.get(endurl, headers=headers)
            

            return Response(r.json(), status=status.HTTP_200_OK) 
            
            #return Response(rjson['token']['catalog'][0]['id'], status=status.HTTP_200_OK)

        except:
            # errorDict = {"result": 500}
            errorDict = {"result": 500}
            errorString = str(errorDict).replace("'", '"')
            errorJson = json.loads(errorString)
            return Response(errorString, status=500)
            #return Response(errorJson)
            
#new APIS to communicate with Orion context broker
class CedusGetEntitiesContextBrocker(APIView):

    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def get(self, request, *args, **kwargs):
        

        endpoint = request.GET.get('endpoint')
        print("endpoint " + endpoint)
                    
        fiware_service = request.GET.get('fiware-service')
        fiware_servicepath = request.GET.get('fiware-servicepath')
       
        headers = {'fiware-service': fiware_service, 'fiware-servicepath': fiware_servicepath}
        #headers = {}

        r = requests.get(endpoint, headers=headers)

        print(r)
        
        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        else:
            return Response({'error': r.status_code},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)        
        
        #return Response({'testget': "hola GET"}, status=status.HTTP_200_OK)

  
#new APIS to communicate with Orion context broker
class CedusPostSubscriptionContextBrocker(APIView):

    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def post(self, request, *args, **kwargs):
        
        print("CedusPostSubscriptionContextBrocker")

        endpoint = request.GET.get('endpoint')
        print("endpoint " + endpoint)
                    
        fiware_service = request.GET.get('fiware-service')
        print("fiware_service " + fiware_service)
        
        fiware_servicepath = request.GET.get('fiware-servicepath')
        print("fiware_servicepath " + fiware_servicepath)
        
        
        headers = {'fiware-service': fiware_service, 'fiware-servicepath': fiware_servicepath, 'Content-Type':'application/json'}
        
        #dataToPost = json.dumps({"entities": [{"id": "kpi_malaga_01","type": "KeyPerformanceIndicator","isPattern": "false"}],"reference": "http://localhost:8000/api/v1/datasetmanager/cedus/dataContextBrocker","duration": "P6M","notifyConditions": [{"type": "ONCHANGE","condValues": ["dateModified"]}],"throttling": "PT5S"})
       
        if (request.POST.dict()):
            print("is a dict")
            bodyQD = request.POST.dict()
            bodyContent = bodyQD['_content']
            body = json.loads(bodyContent)
            
        else:
            print("is not a dict")
            body_unicode = request.body.decode('utf-8')
            body = json.loads(body_unicode)
            
        print("body")
        print(body)
        
        dataToPost = request.body

        print("dataToPost")
        print(dataToPost)

        
        r = requests.post(endpoint, dataToPost, headers=headers)

        print(r)
        print(r.json())
        
        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        else:
            return Response({'error': r.status_code},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)        


#new APIS to communicate with Orion context broker
class CedusDeleteSubscriptionContextBrocker(APIView):

    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def delete(self, request, *args, **kwargs):
        
        print("CedusDeleteSubscriptionContextBrocker")

        endpoint = request.GET.get('endpoint')
        print("endpoint " + endpoint)
                    
        fiware_service = request.GET.get('fiware-service')
        print("fiware_service " + fiware_service)
        
        fiware_servicepath = request.GET.get('fiware-servicepath')
        print("fiware_servicepath " + fiware_servicepath)
        
        headers = {'fiware-service': fiware_service, 'fiware-servicepath': fiware_servicepath}
        
        r = requests.delete(endpoint, headers=headers)

        print(r)
        #print(r.json())
        
        if r.status_code == 200:
            return Response(r.json(), status=status.HTTP_200_OK)
        elif r.status_code == 204:
            return Response({"delete":"0k"}, status=r.status_code)
        else:
            return Response({'error': r.status_code},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)        




class CedusPostDataContextBrocker(APIView):
    # FIXME Potential DDoS Source. Remove once EDP Auth is gone.
    def get(self, request, *args, **kwargs):
        return Response({'testget': "hola GET"}, status=status.HTTP_200_OK)
    
    def post(self, request, *args, **kwargs):
        
        #try:
        if (1==1):
    
            if (request.POST.dict()):
                bodyQD = request.POST.dict()
                print(bodyQD)
                print("---------")
                bodyContent = bodyQD['_content']
                print(bodyContent)
                print("---------")            
                body = json.loads(bodyContent)
                
            else:
                body_unicode = request.body.decode('utf-8')
                body = json.loads(body_unicode)
            
            print("body")
            print(body)
            
            now = datetime.datetime.now()
            #print("now-------------------------------------------------")
            #print(now)
            directory = 'CBDATA'
            if not os.path.exists(directory):
                os.makedirs(directory)
    
            f = open(directory+'/CB_data_'+str(now)+'.txt','w')            
            f.write(str(body))            
            f.close()

            
            print("body['subscriptionId']")
            print(body['subscriptionId'])        
            print ("-------------")


            print("body['attrvalue']")
            print(body['contextResponses'])
            
            for i in range(len(body['contextResponses'])):
                print("i")
                print(i)
                print(body['contextResponses'][i]['contextElement']['attributes'])
                print("-----------------1------------!!!")
                
                for j in range(len(body['contextResponses'][i]['contextElement']['attributes'])):
                    print("j")
                    print(body['contextResponses'][i]['contextElement']['attributes'][j])
                    print("____________________________!!!!!!!!!")
                    
                    if (body['contextResponses'][i]['contextElement']['attributes'][j]['name']=='kpiValue'):
                        dataToinsert = body['contextResponses'][i]['contextElement']['attributes'][j]['value']
                    elif (body['contextResponses'][i]['contextElement']['attributes'][j]['name']=='dateModified'):
                        recvtime = body['contextResponses'][i]['contextElement']['attributes'][j]['value']
                    
    
                
                
            
            print ("-------------")
            #dataToinsert = float(body['contextResponses']['contextElement'])
            print("dataToinsert")
            print(dataToinsert)
            print ("-------------")
            #print("body['recvtime']")
            #print(body['recvtime'])
            
            print("recvtime")
            print(recvtime)
                    
            print ("-------------")
            
            serializer_class = DetailDatasetSerializer
            
            #month resolution
            print("subscriptionId -------")
            print(body['subscriptionId'])
            
            subscriptionId = body['subscriptionId']
            
            #queryset = Dataset.objects.filter(subscription_id='5a79b9ec6bafd4584061982b')
            queryset = Dataset.objects.filter(subscription_id=subscriptionId)
            
            #Iqueryset = Dataset.objects.filter(id=13)
            #year resolution
            #queryset = Dataset.objects.filter(id=69)        
            #queryset = Dataset.objects.filter(id=71)
            #queryset = Dataset.objects.filter(description='a') 
            
            #print("queryset")
            #print(queryset)
            
            #print("len(queryset)")
            #print(len(queryset))
            
            for x in queryset:
                print("x id")
                print(x.id)
                print ("-------------")
                print("x data")
                print(x.data)
                print ("-------------")
                print("x time_end")
                print(x.time_end)
                print ("-------------")
                
                startDateDataset = x.time_start
                endDateDataset = x.time_end
                        
                d = json.loads(x.data)
                
                print("resolution")
                print(d['resolution'])
    
    
                #receivedDate = str(body['recvtime']).split('T')
                receivedDate = str(recvtime).split('T')            
                print ("receivedDate[0]")
                print (receivedDate[0])
                #format YYYY-MM-DD 
                
                arrayDateIn = receivedDate[0].split('-') 
                
                finalResolution = "%Y-%m-%d"
                
                dateImput=arrayDateIn[0]
                resolution = d['resolution']
                
                print("resolution-------")
                print(resolution)
                
                freq='AS'
                
                if (d['resolution']=='year'):
                    freq='AS'
                    finalResolution = "%Y"
                    startDateDataset = x.time_start + "-01-01"
                    endDateDataset = x.time_end + "-01-01"
                    dateImput = arrayDateIn[0] + "-01-01"                
                    
                elif (d['resolution']=='month'):
                    freq='MS'
                    finalResolution = "%Y-%m"
                    startDateDataset = x.time_start + "-01"
                    endDateDataset = x.time_end + "-01"
                    dateImput = arrayDateIn[0]+"-"+arrayDateIn[1]+"-01"
                    
                elif (d['resolution']=='day'):
                    freq='D'
                    finalResolution = "%Y-%m-%d"                                
                    startDateDataset = x.time_start
                    endDateDataset = x.time_end
                    dateImput=arrayDateIn[0]+"-"+arrayDateIn[1]+"-"+arrayDateIn[2]
    
                #a = dt.strptime(row[0], "%Y-%m-%d")
                #b = dt.strptime(str(dataIn), "%Y-%m-%d")            
                
                print("startDateDataset")
                print(startDateDataset)
                print ("-------------")
                print("endDateDataset")
                print(endDateDataset)
                print ("-------------")
                print("dateImput")
                print(dateImput)
                print ("-------------")
                
                newEndDataset = endDateDataset
                
                
                print("dateImput")
                print(dateImput)
                
                a = dt.strptime(endDateDataset, "%Y-%m-%d")
                b = dt.strptime(str(dateImput), "%Y-%m-%d")
            
            
                if (b>a):
                    newEndDataset = dateImput
                
    
                print("newEndDataset")
                print(newEndDataset)
                print ("-------------")
                
                
                array_start_date = startDateDataset.split("-")
                array_end_date = newEndDataset.split("-")
                
                tmpjson = d['data_frame']
                tmpjson = tmpjson.replace("None", 'null')
                
                jsonDataFrame = json.loads(tmpjson)
                
                print ("jsonDataFrame before check")
                print (jsonDataFrame)
                
                new_row_json = {}
                  
                for dataFrame  in jsonDataFrame:
                    row_id = dataFrame
                    
                    print ("dataFrame")
                    print (dataFrame)
                    print ("-------------")
                    individualData = jsonDataFrame[dataFrame]
                    
                    print("individualData data")
                    print(individualData)
                    print ("-------------")
                
    
                    start_date = datetime.date(int(array_start_date[0]), int(array_start_date[1]), int(array_start_date[2]))
                    end_date = datetime.date(int(array_end_date[0]), int(array_end_date[1]), int(array_end_date[2]))
    
    
                    daterange = pd.date_range(start=start_date, end=end_date, freq=freq)
                    
                    dateImputFormat = dateImput+'T00:00:00.000Z'
                    
                    for single_date in daterange:
                        print (single_date.strftime("%Y-%m-%d"))
                        
                        new_date_to_check =  single_date.strftime("%Y-%m-%d")+'T00:00:00.000Z'
                        
                        if (new_date_to_check in jsonDataFrame[dataFrame]):
                            #print (new_date_to_check)
                            print("existe")
                            print(jsonDataFrame[dataFrame][new_date_to_check])
                            
                            if (dateImputFormat==new_date_to_check):
                                jsonDataFrame[dataFrame][new_date_to_check] = dataToinsert
                        else:
                            print("NO existe")
                            
                            
                            #print("dateImputFormat")
                            #print(dateImputFormat)
                            
                            #print("new_date_to_check")
                            #print(new_date_to_check)
                            
                            if (dateImputFormat==new_date_to_check):
                                jsonDataFrame[dataFrame][new_date_to_check] = dataToinsert
                            else:
                                jsonDataFrame[dataFrame][new_date_to_check] = 'null'
                        
     
                    new_row_json[row_id] = jsonDataFrame[dataFrame]
    
    
                new_row_json_string = str(new_row_json)
                new_row_json_string = new_row_json_string.replace("'", '\\"')
                final_json = {"data_frame": new_row_json_string, "resolution": d['resolution'], "unit": d['unit']}
                            
                final_json_as_string = str(final_json)
                final_json_as_string = final_json_as_string.replace("'", '"')
                final_json_as_string = final_json_as_string.replace("\\\\", "\\")            
                final_json_as_string = final_json_as_string.replace('\\"null\\"', "null")
                final_json_as_string = final_json_as_string.replace('None', "null")
    
                print("final_json_as_string")
                print(final_json_as_string)
                
                print("newEndDataset")
                print(newEndDataset)
                print ("-------------")
                endDateToSet = newEndDataset
                
                arrayEndDate = newEndDataset.split('-')
                
                if (d['resolution']=='year'):
                    endDateToSet = arrayEndDate[0]
                    
                elif (d['resolution']=='month'):
                    endDateToSet = arrayEndDate[0]+"-"+arrayEndDate[1]
                    
                elif (d['resolution']=='day'):
                    endDateToSet = newEndDataset
    
    
                print("endDateToSet")
                print(endDateToSet)
                print ("-------------")
                                
                x.time_end = endDateToSet
                x.data = final_json_as_string
                x.save()
             
            return Response({'message': "Ok"}, status=status.HTTP_200_OK)
        
        '''
        except Exception as e:
            print("exception")
            print(e)
            
            return Response({'error': 'error'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        '''