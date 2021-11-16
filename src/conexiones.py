## Test conexiones
import json
import pandas as pd
import simple_salesforce as sf
from simple_salesforce import Salesforce, SFType, SalesforceLogin

"""
-Ver si existe la posibilidad de eliminaciones masivas
Query:
Insert:
Update: SIEMPRE hay que llamar el ID del registro a modificar
Delete:
"""

#Conexion a SF

#Credenciales

login = json.load(open('credenciales.json'))
username = login['username']
password = login['password']
security_token = login['security_token']
domain = 'login'

# Me conecto a la org de SF
session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token, domain=domain)

## Creo una instancia de sf 
sf = Salesforce(instance=instance, session_id=session_id)

def instancia(obj_name):
    """
    :param objname: nombre de un objeto de SF (ej.: objeto__c)
    :return: un objeto asociado al objeto de SF
    """
    object_inst =  SFType(obj_name, session_id, instance)
    return object_inst

##################################################################
contactos = instancia('Contact')

# Obtengo metadata
obj_metadata = sf[nombre_objeto].describe() # TODO Alen: Revisar validez de la sintaxis
df_contactos_metadata = pd.DataFrame(obj_metadata.get('fields')) # Devuelve un dict pero lo convertimos

# Salesforce 
## le tengo que pasar los campos que quiero, no puedo poner 'SELECT *' como en SQL
query = 'SELECT Id, AccountId, id_db__c FROM Contact'
## TODO Alén: leer documentación
sf_contactos = sf.bulk.Contact.query(query)
sf_contactos = pd.DataFrame(sf_contactos)

## Para enviar a SF tengo que reconvertir a diccionario y envio via bulk
benef = primer_migracion.to_dict('records')
resultado = sf.bulk.Contact.insert(benef)
print(resultado) # -----> Esto me muestra qué errores hubo
###################################################################