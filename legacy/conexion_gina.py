import json
import pandas as pd
from simple_salesforce import Salesforce, SalesforceLogin, SFType
import pyodbc 
import pandas as pd

#Conexion a SF

#Credenciales

login = json.load(open('credenciales.json'))
username = login['username']
password = login['password']
security_token = login['security_token']
domain = 'login'

session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token, domain=domain)

##creo una instancia de sf 
sf= Salesforce(instance=instance, session_id=session_id)

##defino la funcion instancia
def instancia(obj_name):
    object_inst = SFType(obj_name, session_id, instance)
    return object_inst



#Conexion a la Base de Datos

def connect(nombre_db):
    servidor = "FPC14"
    nombre_db = nombre_db
    conexion = None
    try:
        conexion = pyodbc.connect("DRIVER={SQL Server Native Client 11.0};"
                                    "Server="+servidor+";"
                                    "DATABASE="+nombre_db+";"
                                    "Trusted_Connection=yes;")
        print("Conexión exitosa")

    except Exception as e:
        print("Ocurrió un error al conectar a SQL Server: ", e)
    
    return conexion

def select(conn, query):
    c = conn.cursor()
    c.execute(query).fetchall
    conn.close

def datos(query, nombre_db):
    conn = connect(nombre_db)
    cursor = conn.cursor()
    df = pd.read_sql_query(query, conn)
    return df 