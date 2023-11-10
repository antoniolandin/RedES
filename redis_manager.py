import redis

class redisManager():
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        print(self.r.acl_whoami())
        
    def register(self, nombre_completo, nombre_usuario, contraseña, privilegios):
        
        # Verifica si el usuario ya existe

        if nombre_usuario in self.r.acl_users():
            raise("El usuario ya existe")
        else:
            self.r.acl_setuser(username=nombre_usuario, passwords=["+" + contraseña], enabled=True)
            
        

manager = redisManager()

manager.register("Antonio Cabrera", "antonio", "admin", "1234", 1)