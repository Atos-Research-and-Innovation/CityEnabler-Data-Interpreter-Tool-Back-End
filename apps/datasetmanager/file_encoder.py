"""
Converts uploaded files into a tabular data structure
"""
import pandas
import codecs
import csv
import datetime
import logging
import os
from xlrd import open_workbook, XL_CELL_DATE, xldate_as_tuple

log = logging.getLogger(__name__)

__author__ = 'fki'


class FileEncoder(object):
    """
    Converts uploaded files into a tabular data structure
    """
    print ("FileEncoder")
    # Register supported extensions with a function
    supported_extensions = {
        '.csv': '_csv_encode',
        '.tsv': '_tsv_encode',
        '.xlsx': '_xlsx_encode',
        '.xls': '_xlsx_encode'
    }

    # file: InMemoryUploadedFile
    def __init__(self, file, jsonData):
        """
        Initialize file and file_name
        """
        self.file = file
        self.file_name, self.file_ext = os.path.splitext(file.name)
        if len(jsonData) != 0:
            self.jsonData = jsonData

    def is_supported(self):
        """
        Check if there is a converter for the extension
        """
        if self.file_ext in self.supported_extensions:
            return True
        else:
            return False

    def encode(self):
        """
        Class the responsible function and encodes the file.
        Returns the result
        """
        print("FileEncoder encode 1")
        print("self.file_ext")
        print(self.file_ext)
        print("self.supported_extensions")
        print(self.supported_extensions)
        print("self.supported_extensions[self.file_ext]")
        print(self.supported_extensions[self.file_ext])
        
        result = getattr(self, self.supported_extensions[self.file_ext])()

        return result

    def _csv_encode(self, delimiter=','):
        """
        Encodes a CSV file.
        """
        print("_csv_encode")
        
        try:
            try:
                #csvdata = pandas.read_csv(self.jsonData['result']['url'])
                csvdata = pandas.read_csv(self.jsonData['result']['url'], header=None, encoding='utf-8', low_memory=False, error_bad_lines=False,  sep=',')
                if ';' in csvdata.values[len(csvdata.values) / 2][0]:
                    #csvdata = pandas.read_csv(self.jsonData['result']['url'], sep=';')
                    csvdata = pandas.read_csv(self.jsonData['result']['url'], header=None, encoding='utf-8', low_memory=False, error_bad_lines=False,  sep=';')
            except:
                csvdata = pandas.read_csv(self.jsonData['result']['url'], quoting=3)
            
            print("continue _csv_encode")    
            colHeadersValues = []

            for q in range(0, len(csvdata.axes[1])):
                colHeadersValues.append(csvdata.axes[1][q])

            completeArray = []
            completeArray.append(colHeadersValues)

            rowIndex = 0
            
            for x in range(0, len(csvdata.values)):
                rowArray = []
                rowIndex = rowIndex + 1
                for y in range(0, len(csvdata.values[x])):
                    if pandas.isnull(csvdata.values[x][y]):
                        rowArray.append("")
                    else:
                        rowArray.append(csvdata.values[x][y])
                completeArray.append(rowArray)

            return completeArray
        except:
            print ("except 1")
            
            print ("delimiter")
            print (delimiter)
            
            r = []
            reader = csv.reader(codecs.iterdecode(self.file, "utf-8"),
                              delimiter=delimiter)
                        
            for row in reader:
                print("for 2nd reader")
                originalrow = row
                print(len(row))
                log.debug(str(row))
                
                if (len(row)<=1):
                    print ("delimiter")
                    print (delimiter)
                    if (delimiter==","):
                        newdelimiter = ";"
                    elif (delimiter==";"):
                        newdelimiter = ","
                    else:
                        newdelimiter = ";"
                    
                    print ("newdelimiter")
                    print (newdelimiter)
                    
                    row = (str(row).split(str(newdelimiter)))
                    row = row[1:]                    
                    row = row[:-1]
                    
                    print (len(row))
                    
                    if (len(row)<=1):
                        print ("is a TAB???")
                        newdelimiter = "\\t"
                        row = (str(originalrow).split(str(newdelimiter)))
                        
                        print ("row")
                        print (row)
                        
                        row = row[1:]                    
                        row = row[:-1]
                        
                        if (len(row)==1):
                            row = originalrow
                        
                log.debug(str(row))
                
                r.append(row)
            return r
            
        
    def _tsv_encode(self):
        """
        Encodes a TSV file
        :return: array of data rows
        """
        try:
            tsvdata = pandas.read_csv(self.jsonData['result']['url'], sep='\t')

            colHeadersValues = []

            for q in range(0, len(tsvdata.axes[1])):
                colHeadersValues.append(tsvdata.axes[1][q])

            completeArray = []
            completeArray.append(colHeadersValues)

            rowIndex = 0
            for x in range(0, len(tsvdata.values)):
                rowArray = []
                rowIndex = rowIndex + 1
                for y in range(0, len(tsvdata.values[x])):
                    if pandas.isnull(tsvdata.values[x][y]):
                        rowArray.append("")
                    else:
                        rowArray.append(tsvdata.values[x][y])
                completeArray.append(rowArray)

            return completeArray
        except:
            return self._csv_encode(delimiter='\t')

    def _xlsx_encode(self):
        """
        Encodes XLS and XLSX files.
        """
        r = []
        wb = open_workbook(file_contents=self.file.read())
        sheet = wb.sheet_by_index(0)
        for row in range(sheet.nrows):
            values = []
            for col in range(sheet.ncols):
                cell = sheet.cell(row, col)
                # Date cells have to be converted to return as string
                if cell.ctype == XL_CELL_DATE:
                    v = xldate_as_tuple(cell.value, wb.datemode)
                    v = datetime.datetime(*v)
                    v = datetime.date(v.year, v.month, v.day)
                elif isinstance(cell.value, float):
                    if cell.value == int(cell.value):
                        v = str(int(cell.value))
                    else:
                        v = str(cell.value)
                else:
                    v = str(cell.value)

                values.append(v)
            r.append(values)
        return r
