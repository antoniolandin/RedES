__author__ = 'Antonio Cabrera y Alejandro Gómez'

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import time
from typing import Generator, Any
from geojson import Point
import pymongo
import redis
import yaml
import json
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.config_set('maxmemory', '150mb')                    #Esta linea limita la memoria máxima que puede tener la caché
r.config_set('maxmemory-policy', 'allkeys-lru')       #Aqui permitimos que redis elimine los LRU (Least Recently Used) para que se mantenga en 150mb

try:                                        #Aqui podemos comprobar el estado de la conexión
    r.ping()
    print("Se ha establecido conexión con el servidor Redis")
except redis.ConnectionError:
    print("Error de conexión con el servidor Redis")


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
            
        if self.__dict__.get("_id"):    # Si existe el id, se actualiza

            r.expire(str(self.__dict__.get("_id")), 86400)   
            self.db.update_one({"_id": self._id}, {"$set": self.__dict__})

        else:   # Si no existe, se inserta creando un id nuevo en el proceso

            r.setex( str(self.__dict__.get("_id")), 86400 , str(self.__dict__))  #Lo añadimos también a la caché
            self.db.insert_one(self.__dict__)
       
                
    def delete(self) -> None:
        """
        Elimina el modelo de la base de datos
        """
        eliminado = r.delete(self.__dict__.get("_id"))  
        
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
    
    def find_by_id(self, id: str) -> dict | None:
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
        if r.get(id) == 1:              #Si existe en la caché, recarga el tiempo de expiración
            r.expire(id, 86400)
            return r.get(id)            #Tras buscarlo, actualiza la caché y desde ahí lo obtiene
        else:                           #Si no existe, devuelve None
            return None
            
    @classmethod
    def init_class(cls, db_collection: pymongo.collection.Collection, required_vars: set[str], admissible_vars: set[str]) -> None:
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

            r.setex( str(modelo._id), 86400 , str(modelo.__dict__))  #setex permite añadir tiempo de expiración

            yield modelo
            

def initApp(definitions_path: str = "./models.yml", mongodb_uri="mongodb://localhost:27017/", db_name="abd") -> None:
    """ 
    Declara las clases que heredan de Model para cada uno de los 
    modelos de las colecciones definidas en definitions_path.
    Inicializa las clases de los modelos proporcionando las variables 
    admitidas y requeridas para cada una de ellas y la conexión a la
    collecion de la base de datos.
    
    Parameters
    ----------
        definitions_path : str
            ruta al fichero de definiciones de modelos
        mongodb_uri : str
            uri de conexion a la base de datos
        db_name : str
            nombre de la base de datos
    """
    # Inicializar base de datos:
    base_de_datos = pymongo.MongoClient(mongodb_uri)[db_name]
    
    # Obtener las definiciones de modelos del fichero yaml
    with open(definitions_path, "r") as f:
        modelos = yaml.load(f, Loader=yaml.FullLoader)
                
    # Declarar tantas clases modelo como colecciones existan en la base de datos
    # Leer el fichero de definiciones de modelos para obtener las colecciones
    # y las variables admitidas y requeridas para cada una de ellas.
    # Ejemplo de declaracion de modelo para colecion llamada MiModelo
    #globals()["MiModelo"] = type("MiModelo", (Model,),{})
    
    for nombre_coleccion in modelos.keys():
        globals()[nombre_coleccion] = type(nombre_coleccion, (Model,), {})   
        modelo = modelos[nombre_coleccion]
        globals()[nombre_coleccion].init_class(db_collection= base_de_datos[nombre_coleccion], required_vars=modelo["required_vars"], admissible_vars=modelo["admissible_vars"])
    
    # Ignorar el warning de Pylance sobre MiModelo, es incapaz de detectar
    # que se ha declarado la clase en la linea anterior ya que se hace
    # en tiempo de ejecucion.
    
def practica_1():
    # Almacenar los pipelines de las consultas en Q1, Q2, etc. 

    # Q1: Listado de todas personas que han estudiado en la UPM o UAM
    Q1 = [{'$match': {'$or' : [{'universidad': "UPM"}, {'universidad':"UAM"}]}}]

    # Q2: Listado  de  las  diferentes  universidades  en  las  que  han  estudiado  las  personas  residentes en Madrid
    Q2 = [{'$match': {'ciudad': 'Madrid'}},{'$project': {'_id':0,'universidad': 1}}]

    # Q3: Personas que, en la descripción de su perfil, incluye los términos “Big Data” o “Ingeligencia Artificial”
    Q3 = [{'$match' : {'$or': [{'descripcion': "Big Data"},{'descripcion': "Inteligencia Artificial"}]}}]

    # Q4: Guarda  en  una  tabla  nueva  el  listado  de  las  personas  que  ha  terminado  alguno  de  sus  estudios en el 2017 o después
    Q4 = [{'$match': {"estudios.fin": {'$gte': 2017}}}, {'$out':"estudiosFin"}]

    # Q5: Calcular  el  número  medio  de  estudios  realizados  por  las  personas  que  han  trabajado  o  trabajan en la Microsoft
    Q5 = [{'$match': {'trabajos': "Microsoft"}}, {'$group': {'_id': "", 'promedio': {'$avg': {'$sum': {'$size': "$estudios"}}}}}]

    # Q6: Distancia media al trabajo (distancia geodésica) de los actuales trabajadores de Google. Se pueden indicar las coordenadas de la oficina de Google manualmente
    Q6 = [{'$geoNear': {'near': { 'type': "Point", 'coordinates': [ 40.4165 ,  -3.7026] },  'distanceField': "dist.calculated"}},{'$match': {'trabajos': "Google"}}, {'$group': {'_id': "",'distancia_media': {'$avg': {'$sum': "$dist.calculated"}}}}, {'$project':{'_id':0}}]

    # Q7: Listado de las tres universidades que más veces aparece como centro de estudios de las personas registradas. Mostrar universidad y el número de veces que aparece
    Q7 = [{'$match': {"universidad" :{ "$ne" : 'null' } } }, {"$group" : {'_id': "$universidad", 'count' :{'$sum':1}}},{'$sort': {'count': -1}},{'$limit':3}]

        
    # Inicializar base de datos y modelos con initApp
    initApp()

    # Hacer pruebas para comprobar que funciona correctamente el modelo

    # Crear modelo
    modelo = MiModelo(nombre="Alberto", apellido="Gutierrez", edad="27")

    # Asignar nuevo valor a variable admitida del objeto 
    modelo.direccion = "Calle de la Reina, 28004 Madrid"

    # Asignar nuevo valor a variable no admitida del objeto 
    print("Asignar nuevo valor a variable no admitida del objeto:")
    
    try:
        modelo.peso = 80
    except ValueError as e:
        print(e)
        
    # Guardar
    modelo.save()
    
    # Asignar nuevo valor a variable admitida del objeto
    modelo.telefono = "+34 650292929"
    
    # Guardar
    modelo.save()
    
    # Buscar nuevo documento con find
    cursor = iter(modelo.find({"nombre": "Alberto"}))
    
    # Obtener primer documento

    primer_documento = next(cursor)
    print(f"\nObtener primer documento:\nTipo:{type(primer_documento)}\nValor_nombre: {primer_documento.nombre}")    
    # Modificar valor de variable admitida
    
    modelo.dni = "12345678A"

    # Guardar
    
    modelo.save()

    #Creamos nuevo modelo

    # Ejecutar consultas Q1, Q2, etc. y mostrarlo

    # Primero vamos a cargar nuestros documentos del archivo data.json
    
    print("\nCargando documentos desde data.json...")
    
    documentos = []
    
    archivo_json = open('data.json')
    
    for modelo in json.load(archivo_json):
        documentos.append(Persona(**modelo))
        documentos[-1].save()   # Guardamos cada documento en la base de datos
    
    archivo_json.close()
    
    print("Documentos cargados correctamente")
    
    # Ahora ejecutamos las consultas
    
    consultas = [Q1, Q2, Q3, Q4, Q5, Q6, Q7]


    # Ejecutamos los comandos y mostramos los resultados
    
    print("\nComandos:")
    
    Persona.db.create_index([('direccion', pymongo.GEOSPHERE)])    # Creamos el indice geoespacial para la consulta Q6 (si no lo creamos, la consulta no puede calcular la distancia)
      
    for consulta in consultas:
        
        print(f"\nQ{consultas.index(consulta) + 1}:")
        
        print(f"Comando: {consulta}")
        print("Resultados: ")
        
        Qr = Persona.aggregate(consulta)
        
        numero_resultados = 0
        
        for resultado in Qr:
            print(resultado)
            numero_resultados += 1
            
        print(f"Total: {numero_resultados} resultados")

def practica2_cache():
    initApp()

    modelo = MiModelo(nombre="Alberto", apellido="Gutierrez", edad="27")
    modelo.direccion = "Calle de la Reina, 28004 Madrid"
    modelo.save()
    print("\nCargando documentos desde data.json...")
    documentos = []
    archivo_json = open('data.json')
    for modelo in json.load(archivo_json):
        documentos.append(Persona(**modelo))
        documentos[-1].save()   # Guardamos cada documento en la base de datos
    archivo_json.close()
    print("Documentos cargados correctamente")

    delete(modelo._id)
    


if __name__ == "__main__":
    
    initApp()
    practica2_cache()