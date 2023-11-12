import redis
import uuid
import pickle

class RedisManager():
    def __init__(self):
        self.db = redis.Redis(host='localhost', port=6379, db=0)
        self.logged = False
        
    def register(self, nombre_usuario, nombre_completo, contraseña, privilegios):
        if(self.db.exists(nombre_usuario)):
            raise("El usuario ya existe")
        else:
            self.db.hset(nombre_usuario, "nombre_completo", nombre_completo)
            self.db.hset(nombre_usuario, "contraseña", contraseña)
            self.db.hset(nombre_usuario, "privilegios", privilegios)
     
    def generate_token(self, nombre_usuario, contraseña):
        if(self.db.hget(nombre_usuario, "contraseña").decode('UTF-8') == contraseña):
            token = str(uuid.uuid4())
            ttl = 60 * 60 * 24 * 30 # 30 días
            
            user_info = {"nombre_usuario": nombre_usuario, "contraseña": contraseña}
            
            self.db.setex(token, ttl, pickle.dumps(user_info))
            
            return token
        else:
            raise("Usuario o contraseña incorrectos")
     
    def login(self, nombre_usuario, contraseña):
        if self.db.hget(nombre_usuario, "contraseña").decode('UTF-8') == contraseña:
            
            privilegios = self.db.hget(nombre_usuario, "privilegios").decode('UTF-8')
            
            self.logged = True
            
            return privilegios
            
        else:
            return -1
     
    def login_and_generate_token(self, nombre_usuario, contraseña):
        if self.db.hget(nombre_usuario, "contraseña").decode('UTF-8') == contraseña:
            
            privilegios = self.db.hget(nombre_usuario, "privilegios").decode('UTF-8')  
            token = self.generate_token(nombre_usuario, contraseña)
            
            self.logged = True
            
            return privilegios, token
            
        else:
            return -1
        
    def login_with_token(self, token):
        if(self.db.exists(token)):
            
            user_info = pickle.loads(self.db.get(token))
            
            return self.login(user_info["nombre_usuario"], user_info["contraseña"])       

        else:
            return -1
        
manager = RedisManager()

print(manager.login_with_token("d33c2b68-a09f-424a-8814-1a3090ca4891"))