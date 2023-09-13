import csv
import mysql.connector
import pandas as pd
from pyspark.shell import spark
from pyspark.sql.functions import *

# Parâmetros de conexão com o banco de dados MySQL (sem usuário e senha)
db_params = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'database': 'dbTeste'
}

con = mysql.connector.connect(host='localhost', database='dbTeste', user='root', password='')


def con_db_execute():
    if con.is_connected():
        db_info = con.get_server_info()
        print("Conectado ao servidor MySQL", db_info)
        cursor = con.cursor()
        linha = cursor.fetchone()
        print("Conectado ao banco de dados", linha)

    if con.is_connected():
        cursor.close()
        con.close()
        print("Conexão MySQL encerrada")

    return


con_db_execute()

if __name__ == '__main__':
    print('Fim!!!')
