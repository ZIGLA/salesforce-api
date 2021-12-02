import json
from tkinter import filedialog
import pandas as pd
from simple_salesforce import Salesforce, SFType, SalesforceLogin

KEYS = {'username','password','security_token'}

class SFConnection():
    """Objeto de conexion a Salesforce

    Crea una conexion en funcion de las credenciales
    que se le provean y efectúa consultas u operaciones
    sobre sus objetos
    """

    def __init__(self, json_creds=None, domain='login'):
        """Crea una instancia del objeto sfConnections

        Args:
            json_creds (str): Path donde se encuentra el archivo JSON con las credenciales.
            Deben corresponderse con las del entorno al que se va a acceder. Si se accede
            a uno de desarrollo (testing) deben usarse esas credenciales.
            domain (str): Dominio al que conectarse. Es 'login' por omisión y debe ser cambiado
            a 'test' para funcionar en entornos de testing/desarrollo.
        Returns:
            Objeto de sesion de SF
        """
        super().__init__()
        if json_creds is not None:
            self.credentials = json.load(open(json_creds, encoding='utf-8'))
        elif self.credentials is None:
            raise RuntimeError('No hay credenciales inicializadas.')
        if set(self.credentials.keys()) != KEYS:
            _keys = list(KEYS)
            _keys.sort()
            raise KeyError(f"Credentials do not match the required keys: {','.join(_keys)}.")

        # Credentials keys dont match
        self.session_id, self.instance = SalesforceLogin(username=self.credentials['username'],
                                                  password=self.credentials['password'],
                                                  security_token=self.credentials['security_token'],
                                                  domain=domain)

        self.sf_object = Salesforce(instance=self.instance, session_id=self.session_id)

    def get_objeto(self, obj_name:str) -> SFType:
        """Retorna un objeto manipulable de Salesforce

        Args:
            obj_name (str): Nombre de un objeto de SF (ej.: objeto__c)
        Returns:
            SFType: Elemento asociado al objeto de SF
        """
        object_inst =  SFType(obj_name, self.session_id, self.instance)
        return object_inst

    def query(self, nombre_objeto: str, fields: list or str, getid=False, conds=None):
        """Funcion que ejecuta una query en SF

        Realiza consultas al objeto 'source' de sf para los campos 'fields'\n
        Args:
            fields (list or str): Lista de campos que devuelve la query. Si se settea a 'all'
                                  consulta por todos los campos.
            nombre_objeto (str): Objeto del cual obtiene la información y al cual pertenecen
                                 los campos de 'fields'
            getid (bool): Indica si se desea retornar el campo Id de los registros.
            conds (str): condiciones SQL para la consulta. CUIDADO, el código no posee mayores controles.
        Returns:
            pandas DataFrame: Dataframe que contiene la consulta en formato tabular
        """
        if str.isidentifier(nombre_objeto) is False:
            raise ValueError("El nombre del objeto es inválido")
        if conds is not None:
            if ';' in conds:
                raise ValueError("Las condiciones poseen una expresión no permitida.")
        if getid is True:
            try:
                fields.index('Id')
            except ValueError:
                fields = ['Id'] + fields
        if fields == 'all':
            _fields = ','.join(self._get_metadata(nombre_objeto, 'fields')['name'].to_list())
        else:
            _fields = ','.join(fields)
        _query = f"SELECT {_fields} FROM {nombre_objeto}"
        if conds is not None:
            _query += conds
        res = exec(f"self.sf_object.bulk.{nombre_objeto}.query(_query + \';\')")
        res = pd.DataFrame(res)
        return res

    def insert(self, data:dict or pd.DataFrame, nombre_objeto: str):
        """Agrega los registros en 'data' al objeto 'nombre_objeto'
        """
        if str.isidentifier(nombre_objeto) is False:
            raise ValueError("El nombre del objeto es inválido")
        res = eval(f"self.sf_object.bulk.{nombre_objeto}.insert(data)")
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
        if str.isidentifier(nombre_objeto) is False:
            raise ValueError("El nombre del objeto es inválido")
        obj_metadata = eval(f"self.sf_object.{nombre_objeto}.describe()")
        return pd.DataFrame(obj_metadata.get(propiedad))