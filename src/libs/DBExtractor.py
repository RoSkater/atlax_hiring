import json
import pyodbc
import csv
import gzip
import shutil

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
            cursor.execute("select * from Item where ItemId between 1 and 50") ##for testing only from 1 to 50 Item IDs,
            ## USE: "cursor.execute("select * from Item")" for the full database
            data = cursor.fetchall()
            
            header = ['ItemId', 'ItemDocumentNbr','CustomerName','CreateDate', 'UpdateDate']#, 'ItemSource']
            
            with open('list.csv', 'w', encoding='UTF8') as f:
                writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

                writer.writerow(header)
                i = 0
                rows = []
                for row in data: ##discard deleted Item Versions
                    if row[2] != 1:
                        rows.append(row)

                
                for row in rows:
                    
                    customer = row[3]
                    
                    if row == rows[-1] or row[0] != rows[i+1][0]: ##check if it is the last row
                        row = list(row)
                        if customer.startswith("99"): ##ItemSource column addition
                            row.append('Local')
                        
                        else:
                            row.append('External')
                        row = tuple(row)
                        row = row[:4] + row[5:]
                        row = row[:2] + row[3:]
                        
                        writer.writerow(row)

                    i = i + 1
            f.close()     

            with open('list.csv', 'rb') as f_in: ##GZIP compress
                with gzip.open('list.csv.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            f_in.close()
            f_out.close() 
            #
            # End of exercise
        except:
            print("error extracting data from sqlserver")
        finally:        
            if conn: conn.close()