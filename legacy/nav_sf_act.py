import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime
from sqlalchemy import create_engine
import re





########### ME TRAIGO LOS DATOS DE SALESFORCE ##############

transacciones = instancia('Transacciones__c')

query = '''SELECT Id, Estado__c, Cuota__c, N_mero_documento__c, Destino__c, Importado__c, Registrado__c
              FROM Transacciones__c'''

sf_transacciones = sf.bulk.Transacciones__c.query(query)
sf_transacciones = pd.DataFrame(sf_transacciones)

#Aplico los filtros
#sf_transacciones = sf_transacciones[sf_transacciones['Estado__c']=='Aprobado (Pendiente ERP)']
sf_transacciones = sf_transacciones[sf_transacciones['Destino__c']==1]
sf_transacciones = sf_transacciones[sf_transacciones['Importado__c']==1]
sf_transacciones = sf_transacciones[sf_transacciones['Registrado__c']==0]
sf_transacciones = sf_transacciones.loc[(sf_transacciones['Cuota__c']==3)] #Las cuotas que viajaron

print(sf_transacciones)

##########  ME TRAIGO LOS DATOS DEL SQL SERVER  ###########

pagos = '''SELECT CUIT_de_la_cuenta, CBU_a_pagar, Titular_de_la_Cuenta, Nro_documento, Nro_Beneficio, Destino AS Destino__c, 
            Registrado AS Registrado__c, Nro_Documento_Pago AS Nro_Documento_Pago__c,Fecha_AcreditacionOP AS Fecha_AcreditacionOP__c 
            FROM [Fundación Perez Companc$Interfaz Beneficios]
             WHERE Nro_Mov > 511904 
             ORDER BY Nro_Mov DESC''' 

pagos_df = datos(pagos, 'FPCBC13')


# #Quito duplicados (siempre ordenando de manera descendente), esto por si hubo correcciones, etc

#pagos_df = pagos_df.drop_duplicates(pagos_df.columns[pagos_df.columns.isin(['Nro_documento'])],
#                          keep='first')


print(pagos_df)


# ########## UNO LAS DOS TABLAS ##############
nav_a_sf= sf_transacciones.merge(pagos_df, left_on='N_mero_documento__c', right_on='Nro_documento', how='inner')

print(nav_a_sf)

# #Transformo los datos
# nav_a_sf = nav_a_sf.astype({"Registrado__c": int, "Destino__c": int})
# nav_a_sf['Fecha_AcreditacionOP__c'] = nav_a_sf['Fecha_AcreditacionOP__c'].dt.strftime('%Y-%m-%d')

# nav_a_sf = nav_a_sf.where((pd.notnull(nav_a_sf)), None)
# nav_a_sf = nav_a_sf.replace([None],'')

#print(nav_a_sf)

#Elijo las columnas a exportar
#nav_a_sf = nav_a_sf[["Id", "Destino__c", "Registrado__c", "Nro_Documento_Pago__c", "Fecha_AcreditacionOP__c", 'CUIT_de_la_cuenta', 'Titular_de_la_Cuenta', 'CBU_a_Pagar', 'Nro_Beneficio']] ##borar nro doc 
nav_a_sf.to_csv('Pagos_cuil.csv', index = False)

# print(nav_a_sf)

######## ENVIO A SALESFORCE #############


# pagos_update = nav_a_sf.to_dict('records')

# print (pagos_update)
# resultado = sf.bulk.Transacciones__c.update(pagos_update)
# print(resultado)