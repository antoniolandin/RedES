import redis

class redisManager():
    def __init__(self):
        self.default_user = redis.Redis(host='localhost', port=6379, db=0)
        
        self.user_connection = None
        
    def login(self, nombre_usuario, contraseña):
        if nombre_usuario in self.default_user.acl_users():
            creds_provider = redis.UsernamePasswordCredentialProvider(nombre_usuario, contraseña)
            self.user_connection = redis.Redis(host="localhost", port=6379, credential_provider=creds_provider)
            
            print(self.user_connection.set("prueba", "hola"))
            
        else:
            print("El usuario no existe") 
        
    def register(self, nombre_completo, nombre_usuario, contraseña, privilegios):
        
        # Verifica si el usuario ya existe

        if nombre_usuario in self.default_user.acl_users():
            raise("El usuario ya existe")
        else:
            self.default_user.acl_setuser(username=nombre_usuario, passwords=["+" + contraseña], commands=["+@all"], keys=["~*"], enabled=True)
            
manager = redisManager()

manager.default_user.acl_deluser("antonio")

manager.register("Antonio", "antonio", "1234", 1)

manager.login("antonio", "1234")