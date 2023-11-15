from ODM import *
from redis_manager import *
import redis
import yaml

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
    
    # Inicializar cache:
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.config_set('maxmemory', '150mb')                    #Esta linea limita la memoria máxima que puede tener la caché
    r.config_set('maxmemory-policy', 'volatile-lru')  
    
    try:                                        #Aqui podemos comprobar el estado de la conexión
        r.ping()
        print("Se ha establecido conexión con el servidor Redis")
    except redis.ConnectionError:
        print("Error de conexión con el servidor Redis")
    
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
        globals()[nombre_coleccion].init_class(db_collection=base_de_datos[nombre_coleccion], redis_client=r, required_vars=modelo["required_vars"], admissible_vars=modelo["admissible_vars"])
    
    # Ignorar el warning de Pylance sobre MiModelo, es incapaz de detectar
    # que se ha declarado la clase en la linea anterior ya que se hace
    # en tiempo de ejecucion.


if __name__ == "__main__":
    
    # Limpiamos la caché
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    r.flushall()
    
    initApp()

    # Creamos un modelo de pruebas
    modelo = MiModelo(nombre="Alex", apellido="gomez", edad="20")
    
    
    print("\n\n\n1- CACHE:")
    
    #Primero salvamos y vemos que lo guarda en la caché
    print("\nGuardamos el modelo en la base de datos y en la caché")
    modelo.save()                                                                 
    print("Contenido de caché:" , r.get(str(modelo.__dict__.get('_id'))))   #Comrpobamos que se almacenó en caché

    #Buscamos por id el modelo
    print("\nCuando buscamos por id, lo cogerá de la caché")
    print("Buscado mediante id: " , modelo.find_by_id(str(modelo.__dict__.get('_id'))))

    #Borramos el modelo de la caché
    print("\nBorramos el modelo de la caché")
    r.delete(str(modelo.__dict__.get('_id')))
    print("Contenido de caché:" , r.get(str(modelo.__dict__.get('_id'))))   #Comrpobamos que se borró de la caché

    #Buscamos por id el modelo, como no está en caché, lo buscará en mongo y lo subirá a la caché
    print("\nSi no está en cache, lo buscará en mongo y lo subirá a la caché")
    print("Buscado mediante id:" , modelo.find_by_id(str(modelo.__dict__.get('_id'))))
    print("Contenido de caché:" , r.get(str(modelo.__dict__.get('_id'))))   #Comrpobamos que se almacenó en caché

    #Borramos el modelo para que se borre de la caché también
    print("\nBorramos el modelo de la base de datos y de la caché")
    modelo.delete()                                                    
    print("Contenido de caché:" , r.get(str(modelo.__dict__.get('_id'))))   #Comrpobamos que se almacenó en caché

    #Comprobamos que si no está ni en caché ni en mongo, devuelve None
    print("\nSi no está en caché ni en mongo, devuelve None")
    print("Buscado mediante id:" , modelo.find_by_id(str(modelo.__dict__.get('_id'))))
    
    # Parte de registro de usuarios y Help Desk
    
    print("\n\n\n2- SESIONES:")
    
    manager = RedisManager()

    manager.register("antonio", "Antonio Cabrera", "1234", 1) # Registrar un usuario
    print("Información de usuario: ", manager.get_user_info("antonio"))

    print("\nLogin con usuario y contraseña: ")

    privilegios,token = manager.login_and_generate_token("antonio", "1234") # Logearse con el usuario

    print("Token: ", token)
    print("Privilegios: ", privilegios)
    print("Información de usuario: ", manager.get_user_info("antonio"))

    print("\nEditar privilegios del usuario: ")
    manager.edit_user_info("antonio", privilegios=2) # Editar privilegios del usuario
    print("Información de usuario: ", manager.get_user_info("antonio"))

    print("\nLogin con token: ")

    privilegios = manager.login_with_token(token) # Logearse con el token

    print("Privilegios: ", privilegios)
    print("Información de usuario: ", manager.get_user_info("antonio"))

    print("\nCerrar sesión: ")
    manager.logout(token) # Cerrar sesión

    # Help Desk
    print("\n\n\n3- HELP DESK:")

    manager.register("juan", "Juan Pérez", "1234", 2) # Registrar un usuario

    print("\nCrear tickets: ")

    manager.create_ticket("antonio", "No funciona el ordenador", "No enciende", 1) # Crear ticket
    manager.create_ticket("juan", "Redis no funciona", "No se puede conectar", 3) # Crear ticket
    manager.create_ticket("antonio", "No va el internet", "No hay internet", 2) # Crear ticket

    print("\nAtender tickets: ")

    for i in range(3):
        print("Usuario del ticket: " + manager.attend_ticket()) # Atender ticket

    print("\nAtender ticket sin tickets: ")
    manager.attend_ticket() # Atender ticket sin tickets
    
