## Test conexiones
import pandas as pd
import simple_salesforce as sf

from simple_salesforce import Salesforce, SFType, SalesforceLogin
from ast import literal_eval

from sfConnection import sfConnection

"""
-Ver si existe la posibilidad de eliminaciones masivas
Query:
Insert:
Update: SIEMPRE hay que llamar el ID del registro a modificar
Delete:
"""

#Conexion a SF
creds = 'credenciales.json'

## Creo una instancia de sf 
inst = sfConnection(json_creds=creds)

# Obtengo metadata
accion = 'describe()'
nombre_objeto = ""
obj_metadata = literal_eval(f"inst.{nombre_objeto}.{accion}")
df_contactos_metadata = pd.DataFrame(obj_metadata.get('fields')) # Devuelve un dict pero lo convertimos

"""
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
"""