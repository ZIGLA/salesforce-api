import pyodbc
import pandas as pd
import simple_salesforce
import numpy as np
from conexion_gina import *
from api_sf import *


contactos = instancia('Contact')

benef = """  SELECT distinct  Becado.Id AS id_db__c, Becado.Nombre_Razon AS FirstName, Becado.Apellido AS LastName, Becado.FechaNacimiento AS Birthdate, Becado.NumDocumento AS DNI__c, Becado.Cuil_Cuit AS Cuil__c,
				Becado.Genero AS Genero__c, Becado.Tel AS HomePhone, Becado.Cel AS MobilePhone, Becado.Email AS Email, Domicilio.Direccion AS MailingStreet,
				Localidad.Nombre AS MailingCity, Provincia.Nombre AS MailingState, CodigoPostal.Nombre AS MailingPostalCode, EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c, FormaPagoBeneficiario.Banco, FormaPagoBeneficiario.CBU AS CBU__c,
				BENEF.Id AS id_parents, InstitucionEducativa.Id AS id_escuelas

				FROM BECA 
				INNER JOIN InstitucionEducativa ON InstitucionEducativa.Id = Beca.InstitucionEducativa_id
				INNER JOIN DESTINATARIOFINAL DF ON BECA.DESTINATARIOFINAL_ID=DF.ID
				INNER JOIN BENEFICIO BEN ON DF.BENEFICIO_ID=BEN.ID
				INNER JOIN BENEFICIARIO BENEF ON BEN.BENEFICIARIO_ID = BENEF.ID
				INNER JOIN EMPLEADO EMP ON BECA.EMPLEADO_ID=EMP.ID
				INNER JOIN EMPRESA E ON E.ID = EMP.EMPRESA_ID
				INNER JOIN BENEFICIARIO BECADO ON DF.BENEFICIARIO_ID = BECADO.ID 
				LEFT JOIN Domicilio ON BECADO.Domicilio_id = Domicilio.Id
				LEFT JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id
				LEFT JOIN Provincia ON Localidad.Provincia_id = Provincia.Id
				LEFT JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id
				LEFT JOIN EstudiosAlcanzados ON BECADO.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id
				LEFT JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = BECADO.Id


				WHERE BEN.MotivoBeneficio_id = 5  """

#Migro los que estaban mal clasificados 

benef_2 = """SELECT distinct  Becado.Id AS id_db__c, Becado.Nombre_Razon AS FirstName, Becado.Apellido AS LastName, Becado.FechaNacimiento AS Birthdate, Becado.NumDocumento AS DNI__c, Becado.Cuil_Cuit AS Cuil__c,
				Becado.Genero AS Genero__c, Becado.Tel AS HomePhone, Becado.Cel AS MobilePhone, Becado.Email AS Email, Domicilio.Direccion AS MailingStreet,
				Localidad.Nombre AS MailingCity, Provincia.Nombre AS MailingState, CodigoPostal.Nombre AS MailingPostalCode, EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c, FormaPagoBeneficiario.Banco, FormaPagoBeneficiario.CBU AS CBU__c,
				BENEF.Id AS id_parents, InstitucionEducativa.Id AS id_escuelas

				FROM BECA 
				LEFT JOIN InstitucionEducativa ON InstitucionEducativa.Id = Beca.InstitucionEducativa_id
				INNER JOIN DESTINATARIOFINAL DF ON BECA.DESTINATARIOFINAL_ID=DF.ID
				INNER JOIN BENEFICIO BEN ON DF.BENEFICIO_ID=BEN.ID
				INNER JOIN BENEFICIARIO BENEF ON BEN.BENEFICIARIO_ID = BENEF.ID
				INNER JOIN EMPLEADO EMP ON BECA.EMPLEADO_ID=EMP.ID
				INNER JOIN EMPRESA E ON E.ID = EMP.EMPRESA_ID
				INNER JOIN BENEFICIARIO BECADO ON DF.BENEFICIARIO_ID = BECADO.ID 
				LEFT JOIN Domicilio ON BECADO.Domicilio_id = Domicilio.Id
				LEFT JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id
				LEFT JOIN Provincia ON Localidad.Provincia_id = Provincia.Id
				LEFT JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id
				LEFT JOIN EstudiosAlcanzados ON BECADO.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id
				LEFT JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = BECADO.Id


				WHERE BEN.MotivoBeneficio_id <> 5"""


benef_df = """ SELECT distinct  Becado.Id AS id_db__c, Becado.Nombre_Razon AS FirstName, Becado.Apellido AS LastName, Becado.FechaNacimiento AS Birthdate, Becado.NumDocumento AS DNI__c, Becado.Cuil_Cuit AS Cuil__c,
				Becado.Genero AS Genero__c, Becado.Tel AS HomePhone, Becado.Cel AS MobilePhone, Becado.Email AS Email, Domicilio.Direccion AS MailingStreet,
				Localidad.Nombre AS MailingCity, Provincia.Nombre AS MailingState, CodigoPostal.Nombre AS MailingPostalCode, EstudiosAlcanzados.Descripcion AS Maximo_nivel_educativo_alcanzado__c, FormaPagoBeneficiario.Banco, FormaPagoBeneficiario.CBU AS CBU__c,
				BENEF.Id AS id_parents, InstitucionEducativa.Id AS id_escuelas

				FROM BECA 
				LEFT JOIN InstitucionEducativa ON InstitucionEducativa.Id = Beca.InstitucionEducativa_id
				INNER JOIN DESTINATARIOFINAL DF ON BECA.DESTINATARIOFINAL_ID=DF.ID
				INNER JOIN BENEFICIO BEN ON DF.BENEFICIO_ID=BEN.ID
				INNER JOIN BENEFICIARIO BENEF ON BEN.BENEFICIARIO_ID = BENEF.ID
				INNER JOIN EMPLEADO EMP ON BECA.EMPLEADO_ID=EMP.ID
				INNER JOIN EMPRESA E ON E.ID = EMP.EMPRESA_ID
				INNER JOIN BENEFICIARIO BECADO ON DF.BENEFICIARIO_ID = BECADO.ID 
				LEFT JOIN Domicilio ON BECADO.Domicilio_id = Domicilio.Id
				LEFT JOIN Localidad ON Domicilio.Localidad_id = Localidad.Id
				LEFT JOIN Provincia ON Localidad.Provincia_id = Provincia.Id
				LEFT JOIN CodigoPostal ON Domicilio.CodigoPostal_id = CodigoPostal.Id
				LEFT JOIN EstudiosAlcanzados ON BECADO.EstudiosAlcanzados_Id = EstudiosAlcanzados.Id
				LEFT JOIN FormaPagoBeneficiario ON FormaPagoBeneficiario.Beneficiario_id = BECADO.Id

				where BECADO.Id IN (
				select DestinatarioFinal.Beneficiario_id AS becade_id
				from Beca

				inner join DestinatarioFinal on DestinatarioFinal.Id = beca.DestinatarioFinal_id
				inner join Empleado on beca.Empleado_Id = Empleado.Id
				inner join BeneficioVersion on Beca.BeneficioVersion_id = BeneficioVersion.Id
				inner join Beneficio on BeneficioVersion.Beneficio_id = Beneficio.Id
				left join Erogacion on Erogacion.PlanCuotas_id = beca.PlanCuotas_id
				left join CondicionBeca on beca.CondicionBeca_id = CondicionBeca.Id
				left join NivelEducativo on beca.NivelEducativo_id = NivelEducativo.Id
				left join EstadoBeca on beca.EstadoBeca_id = EstadoBeca.Id
				left join TipoTurno on beca.TipoTurno_id = TipoTurno.Id
				left join InstitucionEducativa on beca.InstitucionEducativa_id = InstitucionEducativa.Id
				left join TipoInstitucionEducativa on InstitucionEducativa.TipoInstitucionEducativa_id = TipoInstitucionEducativa.Id

				WHERE year (Beneficio.FechaPedido) >=2010
				) """



benef_df = datos(benef_df, 'Beneficios')
print(benef_df)

bancos = "select Entidad AS Banco__c, Codigo from [Fundación Perez Companc$Entidades]"
bancos_df = datos(bancos, 'FPCBC13')


benef_df = benef_df.merge(bancos_df, left_on='Banco', right_on='Codigo', how='left')

benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])],
                         keep='last')
print(benef_df)
benef_df.to_csv('contactos_beneficiaries.csv', index = False)

#Chequeo por las dudas que ya no esté cargado el el/la benificiarix 

#primer_migracion = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\funciones\becadxs_migradas_22-12.csv', encoding = 'utf-8-sig')

# primer_migracion = primer_migracion[['id_db__c']]

# primer_migracion.rename(columns={'id_db__c': 'id_mig'}, inplace=True)

# benef_df = benef_df.merge(primer_migracion, left_on='id_db__c', right_on='id_mig', how='left')

# benef_df = benef_df[benef_df.id_mig.isnull()]

benef_sf = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\beneficiarios_sf.csv', encoding = 'utf-8-sig', sep = ';') ## 6987 

benef_sf = benef_sf[['id_db']]

benef_sf.rename(columns={'id_db': 'id_mig'}, inplace=True)

benef_df = benef_df.merge(benef_sf, left_on='id_db__c', right_on='id_mig', how='left') #me quedo con los que falta migrar

benef_df = benef_df[benef_df.id_mig.isnull()]


# #Quito duplicados

benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])],
                         keep='last')


## Salesforce 

query = 'SELECT Id, AccountId, id_db__c FROM Contact'
sf_contactos = sf.bulk.Contact.query(query)
sf_contactos = pd.DataFrame(sf_contactos)


# #sf_contactos.to_csv('contactos_sf.csv', index = False)

# #Hago el match

sf_contactos['id_db__c'] = pd.to_numeric(sf_contactos['id_db__c'])


sf_contactos = sf_contactos.rename(columns={'id_db__c':'id_colab'})


benef_df=benef_df.merge(sf_contactos, left_on='id_parents', right_on='id_colab', how='inner')

benef_df = benef_df.drop(['attributes', 'Id', 'Codigo', 'Banco', 'id_colab', 'id_mig'], axis=1)

print(benef_df)


# ###########   Me traigo las escuelas    ###################

query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Account'
sf_escuelas = sf.bulk.Account.query(query)
sf_escuelas = pd.DataFrame(sf_escuelas)
sf_escuelas = sf_escuelas[sf_escuelas['RecordTypeId']=='0124W000001AO14QAG']

# sf_escuelas.to_csv('escuelas_migradas.csv', index = False)

print(sf_escuelas)


# ## Hago el match ### 

sf_escuelas['id_db__c'] = pd.to_numeric(sf_escuelas['id_db__c'])

sf_escuelas = sf_escuelas.rename(columns={'id_db__c':'id_esc'})

benef_df=benef_df.merge(sf_escuelas, left_on='id_escuelas', right_on='id_esc', how='left')

benef_df = benef_df.drop(['attributes', 'Name', 'RecordTypeId', 'id_esc'], axis=1)

# print(benef_df)

benef_df = benef_df.rename(columns={'Id':'npsp__Primary_Affiliation__c'})



benef_df = benef_df.drop_duplicates(benef_df.columns[benef_df.columns.isin(['id_db__c'])],
                         keep='last')

# #Limpieza de la base

estudios = {'No Alcanzados                                     ': 'No está escolarizado/a aun',                                    
'Primarios                                         ': 'Primario Completo',                                         
'Secundarios                                       ': 'Secundario Completo',                                       
'Terciarios                                        ': 'Terciario Completo',                                        
'Universitarios                                    ': 'Universitario Completo',                                    
'Incompletos Primarios                             ': 'Primario Incompleto',                             
'Incompletos Secundarios                           ': 'Secundario Incompleto',                           
'Incompletos Terciarios                            ': 'Terciario Incompleto',                            
'Incompletos Universitarios                        ': 'Universitario Incompleto'} 

benef_df['Maximo_nivel_educativo_alcanzado__c'].replace(estudios, inplace=True)


bancos = {'Banco Columbia' : 'BANCO COLUMBIA S.A.',
'Banco Credicoop'	: 'BANCO CREDICOOP COOPERATIVO LIMITADO',
'Banco de Córdoba'  : 'BANCO DE LA PROVINCIA DE CORDOBA S.A.',
'Banco de Corrientes S.A.' : 'BANCO DE CORRIENTES S.A.',
'Banco de la Nación Argentina' : 'BANCO DE LA NACION ARGENTINA',
'Banco de La Pampa': 'BANCO DE LA PAMPA SOCIEDAD DE ECONOMÍA',
'Banco del Chubut': 'BANCO DEL CHUBUT S.A.',
'Banco Francés': 'BANCO BBVA ARGENTINA S.A.',
'Banco Galicia': 'BANCO DE GALICIA Y BUENOS AIRES S.A.U.',
'Banco Hipotecario S.A': 'BANCO HIPOTECARIO S.A.',
'BANCO ITAU BUEN AYRE': 'BANCO ITAU ARGENTINA S.A.',
'Banco Macro SA': 'BANCO MACRO S.A.',
'Banco Patagonia': 'BANCO PATAGONIA S.A.',
'Banco Provincia de Buenos Aires': 'BANCO DE LA PROVINCIA DE BUENOS AIRES',
'Banco Provincia de Neuquén': 'BANCO PROVINCIA DEL NEUQUÉN SOCIEDAD ANÓNIMA',
'Banco Provincia de Santa Cruz': 'BANCO DE SANTA CRUZ S.A.',
'Banco Regional de Cuyo': 'BANCO SUPERVIELLE S.A',
'Banco Santander Río SA': 'BANCO SANTANDER RIO S.A.',
'Santander Miami CC USD': 'BANCO SANTANDER RIO S.A.',
'Banco Supervielle': 'BANCO SUPERVIELLE S.A',
'Citibank': 'CITIBANK N.A',
'Compañía Financiera Argentina (Efectivo SI)': 'COMPAÑIA FINANCIERA ARGENTINA S.A.',
'HSBC': 'HSBC BANK ARGENTINA S.A.',
'ICBC': 'INDUSTRIAL AND COMMERCIAL BANK OF CHINA',
'Nuevo Banco de Entre Rios': 'NUEVO BANCO DE ENTRE RÍOS S.A.',
'Nuevo Banco de Santa Fe': 'NUEVO BANCO DE SANTA FE SOCIEDAD ANONIMA',
'Nuevo Banco del Chaco S.A.' : 'NUEVO BANCO DEL CHACO S. A.' }

benef_df['Banco__c'].replace(bancos, inplace=True)

benef_df['MailingState'] = benef_df['MailingState'].str.capitalize()

provincias = {'Buenos aires' : 'Provincia de Buenos Aires',
'Capital federal' : 'Ciudad de Buenos Aires',
'Ciudad de buenos aires' : 'Ciudad de Buenos Aires',
'Santa fe' : 'Santa Fé',
'Santa cruz' : 'Santa Cruz',
'Río Negro' : 'Río Negro',
'San juan' : 'San Juan',
'Entre rios' : 'Entre Ríos',
'San luis' : 'San Luis'}

benef_df['MailingState'].replace(provincias, inplace=True)

benef_df['Birthdate'] = benef_df['Birthdate'].dt.strftime('%Y-%m-%d')


benef_df.loc[benef_df['Genero__c'] == 1, 'Genero__c'] = 'Femenino'
benef_df.loc[benef_df['Genero__c'] == 2, 'Genero__c'] = 'Masculino'
benef_df.loc[benef_df['Genero__c'] == 0, 'Genero__c'] = ''
benef_df.loc[benef_df['Genero__c'] == None, 'Genero__c'] = ''

benef_df['Cuil__c'] = benef_df['Cuil__c'].str.replace(r'\D', '')
benef_df['HomePhone'] = benef_df['HomePhone'].str.replace(r'\D', '')
benef_df['MobilePhone'] = benef_df['MobilePhone'].str.replace(r'\D', '')


benef_df = benef_df.drop(['id_parents', 'id_escuelas'], axis=1)


# ## MIGRACION ###

benef_df = benef_df.where((pd.notnull(benef_df)), None)
benef_df = benef_df.replace([None],'')


benef_df['RecordTypeId'] = '0124W000001ANfeQAG'
# benef_df['Rol_en_la_Familia__c'] = 'Hijo/a del colaborador'


# print("pre-migración")
print(benef_df)

benef_df.to_csv('benef_migrados-último.csv', index = False)

# #benef_prueba = benef_df.iloc[0:200]
# #print(benef_prueba)
# #benef = benef_prueba.to_dict('records')

# #df = pd.read_excel(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\benef_migrados2.xlsx')


# #df_dic = df.to_dict('records')

# #resultado = sf.bulk.Contact.insert(df_dic)
# #print(resultado)


# ###### DATOS QUE QUEDARON SIN  MIGFRAR #####

# # query = 'SELECT Id, Name, RecordTypeId, id_db__c FROM Contact'
# # sf_benef = sf.bulk.Contact.query(query)
# # sf_benef = pd.DataFrame(sf_benef)
# # sf_benef = sf_benef[sf_benef['RecordTypeId']=='0124W000001ANfeQAG'
# # sf_benef = sf_benef[sf_benef['id_db__c'].notnull()]

# # sf_benef['id_db__c'] = pd.to_numeric(sf_benef['id_db__c'])

# # sf_benef = sf_benef[['id_db__c']]


# # sf_benef.rename(columns={'id_db__c': 'id_mig'}, inplace=True)

# # benef_df = benef_df.merge(sf_benef, left_on='id_db__c', right_on='id_mig', how='left')

# # benef_df = benef_df[benef_df.id_mig.isnull()]

# benef_df1 = benef_df[benef_df.Birthdate.notnull()]

# benef_df1 = benef_df[benef_df.Birthdate.isnull()]
# benef_df2 = benef_df[benef_df['Birthdate'] == '']

# # benef_df = benef_df.drop(['id_mig'], axis=1)

# #benef_df2.to_csv('benef_sinmigrar.csv', index = False)

# #print(benef_df)

# primer_migracion = pd.read_csv(r'C:\Users\NAV2019VOXA\Downloads\ZIGLA\FPC\funciones\benef_sinmigrar.csv', encoding = 'utf-8-sig')


# primer_migracion['Birthdate'] = '1973-01-01'

# primer_migracion = primer_migracion[primer_migracion.DNI__c.notnull()]


# primer_migracion = primer_migracion.where((pd.notnull(primer_migracion)), None)
# primer_migracion = primer_migracion.replace([None],'')

# primer_migracion['DNI__c'] = primer_migracion.DNI__c.astype(int)

# print(primer_migracion)

# benef = primer_migracion.to_dict('records')
# resultado = sf.bulk.Contact.insert(benef)
# print(resultado)
