from bz2 import compress
import json
import pyodbc
import csv
import gzip

class DBExtractor():
    def __init__(self, configFile: str):
        f = open(configFile)
        data = json.load(f)
        
        self._HOST = data["HOST"]
        self._PORT = data["PORT"]
        self._DATABASE = data["DATABASE"]
        self._USER = data["USER"]
        self._PASSWORD = data["PASSWORD"]
        
        f.close()
 

    def extract(self, targetFile: str):
        conn = None
        
        try:
            conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server}" +
                                ";SERVER=" + self._HOST + 
                                ";DATABASE=" + self._DATABASE + 
                                ";UID=" + self._USER + 
                                ";PWD=" + self._PASSWORD + 
                                ";TrustServerCertificate=Yes")
            
            # Insert your exercise code here
            #
            print("CONNECTION OK")
            cursor = conn.cursor()
            cursor.execute("select * from Item where ItemId between 1 and 10")
            data = cursor.fetchall()
            
            header = ['ItemId', 'ItemDocumentNbr','CustomerName','CreateDate', 'UpdateDate']
            
            with open('list.csv', 'w', encoding='UTF8') as f:
                writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

                writer.writerow(header)
                i = 0
                rows = []
                for row in data: 
                    if row[2] != 1:
                        rows.append(row)

                
                for row in rows:
                    if row == rows[-1]:
                        
                        row = row[:4] + row[5:]
                        row = row[:2] + row[3:]
                        
                        writer.writerow(row)
                    elif row[0] != rows[i+1][0]: 
                        
                        row = row[:4] + row[5:]
                        row = row[:2] + row[3:]
                        
                        writer.writerow(row)

                    i = i + 1
            f.close()     
            
            
            #
            # End of exercise
        except:
            print("error extracting data from sqlserver")
        finally:        
            if conn: conn.close()