import json
from tkinter import filedialog
import pandas as pd
from pandas.core.indexing import check_bool_indexer
from simple_salesforce import Salesforce, SFType, SalesforceLogin

class SFConnectionJWT():
    def __init__(self, username, consumer_key, privatekey):
        self.sf_object = Salesforce(username=username, consumer_key=consumer_key, privatekey=privatekey)

    def get_objeto(self, nombre_objeto:str) -> SFType:
        """Retorna un objeto manipulable de Salesforce

        Args:
            obj_name (str): Nombre de un objeto de SF (ej.: objeto__c)
        Returns:
            SFType: Elemento asociado al objeto de SF
        """
        object_inst =  SFType(nombre_objeto, self.session_id, self.instance)
        return object_inst

    def query(self, nombre_objeto: str, fields='all', getid=False, conds=None) -> pd.DataFrame:
        """Funcion que ejecuta una query en SF

        Realiza consultas al objeto 'source' de sf para los campos 'fields'
        Args:
            fields (list or str): Lista de campos que devuelve la query. Si se settea a 'all'
                                  consulta por todos los campos excepto los de tipo 'Compound Field'
                                  debido a limitaciones de la API misma.
            nombre_objeto (str): Objeto del cual obtiene la información y al cual pertenecen
                                 los campos de 'fields'
            getid (bool): Indica si se desea retornar el campo Id de los registros.
            conds (str): condiciones SQL para la consulta. CUIDADO, el código no posee
                         mayores controles.
        Returns:
            pandas DataFrame: Dataframe que contiene la consulta en formato tabular
        """
        self._check_obj_valido(nombre_objeto)
        if conds is not None:
            if ';' in conds:
                raise ValueError("Las condiciones poseen una expresión no permitida (';').")

        if isinstance(fields, list) and (getid is True) and ('Id' not in fields):
            fields = ['Id'] + fields

        if fields == 'all':
            _fields = ','.join(self.get_campos(nombre_objeto,not_compound=True)['name'].to_list())
        else:
            _fields = ','.join(fields)

        _query = f"SELECT {_fields} FROM {nombre_objeto} "
        if conds is not None:
            _query += conds

        res = eval(f"self.sf_object.bulk.{nombre_objeto}.query(_query)")
        res = pd.DataFrame(res)
        return res

    def insert(self, data:dict or pd.DataFrame, nombre_objeto: str):
        """Agrega los registros en 'data' al objeto 'nombre_objeto'
        """
        self._check_obj_valido(nombre_objeto)
        res = eval(f"self.sf_object.bulk.{nombre_objeto}.insert(data)")
        return pd.DataFrame(res)

    def upsert(self, data:dict or pd.DataFrame, id_externo, nombre_objeto: str):
        """Agrega los registros en 'data' al objeto 'nombre_objeto' con el id externo 'id_externo'
        """
        self._check_obj_valido(nombre_objeto)
        res = eval(f"self.sf_object.bulk.{nombre_objeto}.upsert(data,id_externo)")
        return pd.DataFrame(res)

    def get_credentials_dialog(self, encoding='utf-8'):
        """Abre un cuadro de dialogo para cargar credenciales

        Las credenciales deben ser un archivo JSON que contenga los
        campos:
            username, password, y security_token

        Args:
            encoding (str): Codificación del archivo a cargar.
        """
        filename = filedialog.askopenfilename(defaultextension='json')
        self.credentials = json.load(open(filename, encoding=encoding)) # Load JSON file

         # Credentials keys dont match
        if set(self.credentials.keys()) != KEYS:
            _keys = list(KEYS)
            _keys.sort()
            raise KeyError(f"Credentials do not match the required keys: {','.join(_keys)}.")

    def _get_metadata(self, nombre_objeto: str, propiedad: str):
        """Devuelve la metadata de un objeto de SF

        Args:
            nombre_objeto (str): Nombre del objeto a ser evaluado.
            propiedad (str): Propiedad de la cual se quiere conocer la metadata.
        Returns:
            pandas DataFrame: Dataframe que contiene la metadata solicitada en formato tabular.
        """
        self._check_obj_valido(nombre_objeto)
        obj_metadata = eval(f"self.sf_object.{nombre_objeto}.describe()")
        return pd.DataFrame(obj_metadata.get(propiedad))

    def _check_obj_valido(self, nombre_obj:str):
        if str.isidentifier(nombre_obj):
            if nombre_obj in [dict(x)['name'] for x in dict(self.sf_object.describe())['sobjects']] == False:
                raise KeyError("El nombre de campo no pertenece a la instancia de Salesforce.")
        else:
            raise ValueError("El nombre del campo es inválido, revise la sintaxis.")
    
    def get_objetos(self) -> list:
        return [x['name'] for x in self.sf_object.describe()["sobjects"]]

    def get_campos(self, nombre_objeto: str, not_compound=False):
        """Retorna un pandas DataFrame con todos los campos, sus nombres y labels
            Args:
                nombre_objeto (str): Nombre del objeto que se quiere describir
            Returns:
                pandas DataFrame: Dataframe con campos y labels correspondientes.
                    'names' son los nombres reales de los campos en el sistema y
                    'labels' los nombres dados por el usuario al crearlos
        """
        self._check_obj_valido(nombre_objeto)
        b = self._get_metadata(nombre_objeto,'fields')
        if not_compound==True:
          b = b[~(b['name'].str.endswith('Address') | b['name'].str.startswith('longitude') | b['name'].str.startswith('latitude'))]
        return b[['name', 'label']]

    def update(self, nombre_objeto: str, condicion_registros: str, dict_cambios:dict):
        self._check_obj_valido(nombre_objeto)
        ids = self.query(nombre_objeto, fields=['Id'], conds=condicion_registros)['Id'].to_list()

        for id in ids:
            eval(f"self.sf_object.{nombre_objeto}.update(id, dict_cambios)")

    def bulk_update(self, nombre_objeto:str, list_cambios:list):
        """Efectúa un update para varios registros.
            Args:
                listt_cambios (lsit): debe contener el Id de los registros y los campos a modificar en
                el siguiente formato: 
                    [{'Id':'string_id1', 'campo1':'nuevo_valor1'},
                     {'Id':'string_id2', 'campo2':'nuevo_valor2'}]
        """
        self._check_obj_valido(nombre_objeto)
        eval(f"self.sf_object.bulk.{nombre_objeto}.update(list_cambios, batch_size=10000,use_serial=True)")
        
    def delete(self, nombre_objeto: str, data: pd.DataFrame):
        self._check_obj_valido(nombre_objeto)
        res = eval(f"self.sf_object.bulk.{nombre_objeto}.delete(data)")
        return pd.DataFrame(res)
