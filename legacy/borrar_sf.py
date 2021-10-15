
import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from datetime import datetime


beneficios = instancia('Beneficios_a_Personas__c')


# query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Beneficios_a_Personas__c'
# sf_benef = sf.bulk.Beneficios_a_Personas__c.query(query)
# sf_benef = pd.DataFrame(sf_benef)
# sf_benef = sf_benef[sf_benef['RecordTypeId']=='0124W000001AcYEQA0']
# sf_benef = sf_benef[sf_benef['id_db__c'].notnull()]

# print(sf_benef)

# sf_benef = sf_benef[['Id']]

# list_id = sf_benef['Id'].tolist()

# for i in list_id:
#     resultado = sf.bulk.Beneficios_a_Personas__c.delete(i)
# print(resultado)





# #sf_escuelas.to_csv('escuelas_migradas.csv', index = False)


# #Borar programas

# # contactos = instancia('Beneficios_a_Personas__c')


# # query = 'SELECT Id, Name, RecordTypeId, id_db__c, Programa__c FROM Beneficios_a_Personas__c'
# # sf_benef = sf.bulk.Beneficios_a_Personas__c.query(query)
# # sf_benef = pd.DataFrame(sf_benef)

# # print(sf_benef)

# # #sf_benef = sf_benef[sf_benef['Programa__c']=='a0r4W00000bjFGlQAM']
# # sf_benef = sf_benef[sf_benef['id_db__c'].notnull()]

# # print(sf_benef)

# # sf_benef = sf_benef[['Id']]

# # list_id = sf_benef['Id'].tolist()

# # print(list_id)

# # for i in list_id:
# #     resultado = sf.Beneficios_a_Personas__c.delete(i)
# #     print(resultado)
