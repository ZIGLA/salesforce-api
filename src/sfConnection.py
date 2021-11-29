import json
import pandas as pd
from tkinter import filedialog
from simple_salesforce import Salesforce, SFType, SalesforceLogin

KEYS = {'username','password','security_token'}

#TODO: Ver si corresponde agregar herencia
class sfConnection():
    """Objeto de conexion a Salesforce

    Crea una conexion en funcion de las credenciales
    que se le provean y efectúa consultas u operaciones
    sobre sus objetos
    """

    def __init__(self, json_creds=None):
        super().__init__()
        credentials =  json.load(open(json_creds, encoding='utf-8'))
        if set(credentials.keys()) != KEYS:
            _keys = list(KEYS)
            _keys.sort()
            raise KeyError(f"Credentials do not match the required keys: {','.join(_keys)}.")

        # Credentials keys dont match
        self.credentials = credentials
        self.session_id, self.instance = SalesforceLogin(username=credentials['username'],
                                                  password=credentials['password'],
                                                  security_token=credentials['security_token'],
                                                  domain='login')

        self.sf = Salesforce(instance=self.instance, session_id=self.session_id)

    def get_objeto(self, obj_name:str) -> SFType:
        """Retorna un objeto manipulable de Salesforce.

        Args:
            obj_name (str): Nombre de un objeto de SF (ej.: objeto__c)
        Returns:
            SFType: Elemento asociado al objeto de SF
        """
        object_inst =  SFType(obj_name, self.session_id, self.instance)
        return object_inst

    def query(self, fields: list, source: str, getid=False):
        """Funcion que ejecuta una query en sf
        
        Realiza consultas al objeto 'source' de sf para los campos 'fields'\n
        Args:
            fields (list): Lista de campos que devuelve la query
            source (str): Objeto del cual obtiene la información y al cual pertenecen
                          los campos de 'fields'
        Returns:
            pandas DataFrame: Dataframe que contiene la consulta en formato tabular
        """
        fields_ = ['Id']
        if getid is True:
            try:
                fields.index('Id')
            except ValueError:
                fields_ += fields
        _query = f"SELECT {','.join(fields_)} FROM {source}"
        sf_contactos = self.sf.bulk.Contact.query(_query)
        sf_contactos = pd.DataFrame(sf_contactos)

    def insert(self, data:dict, nombre_objeto: str):
        """
        Agrega los registros en data al objeto nombre_objeto
        """
        res = self.sf.bulk.Contact.insert(data)
        return pd.DataFrame(res)

    def get_credentials_dialog(self, encoding='utf-8'):
        filename = filedialog.askopenfilename(defaultextension='json')
        self.credentials = json.load(open(filename, encoding=encoding)) # Load JSON file

         # Credentials keys dont match
        if set(self.credentials.keys()) != KEYS:
            _keys = list(KEYS)
            _keys.sort()
            raise KeyError(f"Credentials do not match the required keys: {','.join(_keys)}.")

    def _get_metadata(self, nombre_objeto: str):
        # Obtengo metadata de nombre_objeto
        obj_metadata = self.sf[nombre_objeto].describe() # Sintaxis invalida, se accede con operador .
        df_contactos_metadata = pd.DataFrame(obj_metadata.get('fields')) # Devuelve un dict pero lo convertimos