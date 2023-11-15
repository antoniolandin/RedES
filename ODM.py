__author__ = 'Antonio Cabrera y Alejandro Gómez'

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any
from geojson import Point
import pymongo
import redis
from bson import ObjectId      #Aqui permitimos que redis elimine los LRU (Least Recently Used) para que se mantenga en 150mb

def getLocationPoint(address: str) -> Point:
    """ 
    Obtiene las coordenadas de una dirección en formato geojson.Point
    Utilizar la API de geopy para obtener las coordenadas de la direccion
    Cuidado, la API es publica tiene limite de peticiones, utilizar sleeps.

    Parameters
    ----------
        address : str
            direccion completa de la que obtener las coordenadas
    Returns
    -------
        geojson.Point
            coordenadas del punto de la direccion
    """
    location = None
    while location is None:
        try:
            time.sleep(1)

            # Es necesario proporcionar un user_agent para utilizar la API
            # Utilizar un nombre aleatorio para el user_agent
            location = Nominatim(user_agent="Alberto Gutierrez").geocode(address)
        except GeocoderTimedOut:
            # Puede lanzar una excepcion si se supera el tiempo de espera
            # Volver a intentarlo
            continue
        
    # Devolver un GeoJSON de tipo punto con la latitud y longitud almacenadas
    return Point((location.latitude, location.longitude))

class Model:
    """ 
    Clase de modelo abstracta
    Crear tantas clases que hereden de esta clase como  
    colecciones/modelos se deseen tener en la base de datos.

    Attributes
    ----------
        required_vars : set[str]
            conjunto de variables requeridas por el modelo
        admissible_vars : set[str]
            conjunto de variables admitidas por el modelo
        db : pymongo.collection.Collection
            conexion a la coleccion de la base de datos
    
    Methods
    -------
        __setattr__(name: str, value: str | dict) -> None
            Sobreescribe el metodo de asignacion de valores a las
            variables del objeto con el fin de controlar las variables
            que se asignan al modelo y cuando son modificadas.
        save()  -> None
            Guarda el modelo en la base de datos
        delete() -> None
            Elimina el modelo de la base de datos
        find(filter: dict[str, str | dict]) -> ModelCursor
            Realiza una consulta de lectura en la BBDD.
            Devuelve un cursor de modelos ModelCursor
        aggregate(pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor
            Devuelve el resultado de una consulta aggregate.
        find_by_id(id: str) -> dict | None
            Busca un documento por su id utilizando la cache y lo devuelve.
            Si no se encuentra el documento, devuelve None.
        init_class(db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None
            Inicializa las variables de clase en la inicializacion del sistema.

    """
    required_vars: set[str]
    admissible_vars: set[str]
    db: pymongo.collection.Collection
    r: redis.client.Redis

    def __init__(self, **kwargs: dict[str, str | dict]):
        """
        Inicializa el modelo con los valores proporcionados en kwargs
        Comprueba que los valores proporcionados en kwargs son admitidos
        por el modelo y que las variables requeridas son proporcionadas.

        Parameters
        ----------
            kwargs : dict[str, str | dict]
                diccionario con los valores de las variables del modelo
        """

        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        if not all(name in kwargs for name in self.required_vars):
            print(kwargs, self.required_vars)
            raise ValueError("Faltan variables requeridas")
        
        # Asigna todos los valores en kwargs a las variables con 
        # nombre las claves en kwargs
        for name, value in kwargs.items():
            setattr(self, name, value)          

    def __setattr__(self, name: str, value: str | dict) -> None:
        """ Sobreescribe el metodo de asignacion de valores a las 
        variables del objeto con el fin de controlar las variables
        que se asignan al modelo y cuando son modificadas.
        """

        # Realizar las comprabociones y gestiones necesarias
        # antes de la asignacion.
        
        # Ver si la variable a asignar es admitida
        if not (name in self.admissible_vars or name in self.required_vars):
            raise ValueError("Variable no admitida")
        
        # Ver si el valor a asignar es información nueva
        if name in self.__dict__ and self.__dict__[name] == value:
            raise ValueError("Variable ya asignada, solo se puede enviar información nueva a la BBDD")

        if name == "direccion" and type(value) == str: # Si la direccion nos viene dada como string, la convertimos en un punto
            self.__dict__[name] = getLocationPoint(value)
            return

        # Asigna el valor value a la variable name
        self.__dict__[name] = value
        
    def save(self) -> None:
        """
        Guarda el modelo en la base de datos
        Si el modelo no existe en la base de datos, se crea un nuevo
        documento con los valores del modelo. En caso contrario, se
        actualiza el documento existente con los nuevos valores del
        modelo.
        """
        

        if self.__dict__.get('_id'):    # Si existe el id, se actualiza
            self.db.update_one({"_id": self.__dict__.get("_id")}, {"$set": self.__dict__})

        else:   # Si no existe, se inserta creando un id nuevo en el proceso
            self.db.insert_one(self.__dict__)
       

        key = str(self.__dict__.get('_id'))
        valor = self.r.get(key)

        if valor is not None:
            print("Save: Estaba en caché, reactualizando caché")
            self.r.expire(key, 86400)  
        else:                   #Lo añadimos también a la caché
            print("Save: Añadiendo a caché")
            self.r.setex( key,86400, str(self.__dict__))     



    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
        key = str(self.__dict__.get("_id"))
        eliminado = self.r.delete(key)  
        
        if eliminado > 0:              #Comprobante de que se ha eliminado correctamente un archivo
            print(f"La clave se ha eliminado correctamente de la caché.")
        else:
            print(f"La clave no existe en la caché o no se pudo eliminar.")
     
        self.db.delete_one(self.__dict__)
    


    @classmethod
    def find(cls, filter: dict[str, str | dict]) -> Any:
        """ 
        Utiliza el metodo find de pymongo para realizar una consulta
        de lectura en la BBDD.
        find debe devolver un cursor de modelos ModelCurso

        Parameters
        ----------
            filter : dict[str, str | dict]
                diccionario con el criterio de busqueda de la consulta
        Returns
        -------
            ModelCursor
                cursor de modelos
        """ 

        # cls es el puntero a la clase
        
        return ModelCursor(cls, cls.db.find(filter))


    @classmethod
    def aggregate(cls, pipeline: list[dict]) -> pymongo.command_cursor.CommandCursor:
        """ 
        Devuelve el resultado de una consulta aggregate. 
        No hay nada que hacer en esta funcion.
        Se utilizara para las consultas solicitadas
        en el segundo apartado de la practica.

        Parameters
        ----------
            pipeline : list[dict]
                lista de etapas de la consulta aggregate 
        Returns
        -------
            pymongo.command_cursor.CommandCursor
                cursor de pymongo con el resultado de la consulta
        """ 
        return cls.db.aggregate(pipeline)
    @classmethod
    def find_by_id(cls, id: str) -> dict | None:
        """ 
        NO IMPLEMENTAR HASTA LA SEGUNDA PRACTICA
        Busca un documento por su id utilizando la cache y lo devuelve.
        Si no se encuentra el documento, devuelve None.
        
        Parameters
        ----------
            id : str
                id del documento a buscar
        Returns
        -------
            dict | None
                documento encontrado o None si no se encuentra
        """
        valor = cls.r.get(id)
        documento = cls.db.find_one({"_id": ObjectId(id)})

        if valor is not None:                                  #Si existe en la caché, recarga el tiempo de expiración
            cls.r.expire(id, 86400)
            return cls.r.get(id)                                #Tras buscarlo, actualiza la caché y desde ahí lo obtiene
        else:  
            if documento is not None:    #Si no está en caché la busca en mongo
                cls.r.setex(id, 86400, str(documento))
                return cls.r.get(id) 
            else:          
                print("find_by_id(): No encontrado")                                 #Si no existe, devuelve None            
                return None

    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, redis_client: redis.client.Redis, required_vars: set[str], admissible_vars: set[str]) -> None:
        """ 
        Inicializa las variables de clase en la inicializacion del sistema.
        En principio nada que hacer aqui salvo que se quieran realizar
        comprobaciones o cambios adicionales.

        Parameters
        ----------
            db_collection : pymongo.collection.Collection
                Conexion a la collecion de la base de datos.
            required_vars : set[str]
                Set de variables requeridas por el modelo
            admissible_vars : set[str] 
                Set de variables admitidas por el modelo
        """
        cls.db = db_collection
        cls.r = redis_client
        cls.required_vars = required_vars
        cls.admissible_vars = admissible_vars
        
class ModelCursor:
    """ 
    Cursor para iterar sobre los documentos del resultado de una
    consulta. Los documentos deben ser devueltos en forma de objetos
    modelo.

    Attributes
    ----------
        model_class : Model
            Clase para crear los modelos de los documentos que se iteran.
        cursor : pymongo.cursor.Cursor
            Cursor de pymongo a iterar

    Methods
    -------
        __iter__() -> Generator
            Devuelve un iterador que recorre los elementos del cursor
            y devuelve los documentos en forma de objetos modelo.
    """

    def __init__(self, model_class: Model, cursor: pymongo.cursor.Cursor):
        """
        Inicializa el cursor con la clase de modelo y el cursor de pymongo

        Parameters
        ----------
            model_class : Model
                Clase para crear los modelos de los documentos que se iteran.
            cursor: pymongo.cursor.Cursor
                Cursor de pymongo a iterar
        """
        self.model = model_class
        self.cursor = cursor
    
    def __iter__(self) -> Generator:
        """
        Devuelve un iterador que recorre los elementos del cursor
        y devuelve los documentos en forma de objetos modelo.
        Utilizar yield para generar el iterador
        Utilizar la funcion next para obtener el siguiente documento del cursor
        Utilizar alive para comprobar si existen mas documentos.
        """
        
        while self.cursor.alive:
            siguiente = next(self.cursor)
            modelo = self.model(**siguiente)

            self.r.setex( str(modelo._id), 86400 , str(modelo.__dict__))  #setex permite añadir tiempo de expiración

            yield modelo