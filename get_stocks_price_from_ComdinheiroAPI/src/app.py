import pyodbc
import logging
from datetime import datetime
import sys
sys.path.append('././')
import comdinheiro

inital_data = "01/01/2015"
end_data = "01/01/2022"

LOG = "././log/ccd_history_"+inital_data+"_"+end_data+".log"
logging.basicConfig(filename=LOG, filemode="w", level=logging.DEBUG)

sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server}; \
                            SERVER=YourDbHost; \
                            DATABASE=YourSchema; \
                            UID=YourDbUser; \
                            PWD=YourDbPassword')

comdinheiro.get_from_API(inital_data, end_data, logging, sql_conn)
logging.info("Execucao finalizada com sucesso!")
sql_conn.close()