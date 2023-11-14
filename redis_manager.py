import redis
import uuid
import pickle

class RedisManager():
    def __init__(self):
        self.db = redis.Redis(host='localhost', port=6379, db=0)
        
    def register(self, nombre_usuario, nombre_completo, contraseña, privilegios):
        if(self.db.hexists("usuarios", nombre_usuario)):
            raise("El usuario ya existe")
        else:
            user_info = {"nombre_completo": nombre_completo, "contraseña": contraseña, "privilegios": privilegios}
            self.db.hset("usuarios", nombre_usuario, pickle.dumps(user_info))
            print("Usuario registrado correctamente")
     
    def generate_token(self, nombre_usuario, contraseña):
        
        if not self.db.hexists("usuarios", nombre_usuario):
            raise("El usuario no existe")
        
        user_info = pickle.loads(self.db.hget("usuarios", nombre_usuario))
        
        
        if(user_info["contraseña"] == contraseña):
            token = str(uuid.uuid4())
            ttl = 60 * 60 * 24 * 30 # 30 días
            
            login_info = {"nombre_usuario": nombre_usuario, "contraseña": contraseña}
            
            self.db.setex(token, ttl, pickle.dumps(login_info))
            
            return token
        else:
            raise("Usuario o contraseña incorrectos")
     
    def login(self, nombre_usuario, contraseña):
        
        # Ver si existe el usuario
        if not self.db.hexists("usuarios", nombre_usuario):
            print("El usuario no existe")
            return -1
        
        
        user_info = pickle.loads(self.db.hget("usuarios", nombre_usuario))
        
        if user_info["contraseña"] == contraseña:
            
            privilegios = user_info["privilegios"]
            
            return privilegios   
        else:
            print("Contraseña incorrecta")
            return -1
     
    def login_and_generate_token(self, nombre_usuario, contraseña):
        
        # Ver si existe el usuario
        
        if not self.db.hexists("usuarios", nombre_usuario):
            print("El usuario no existe")
            return -1
        
        user_info = pickle.loads(self.db.hget("usuarios", nombre_usuario))
        
        
        if user_info["contraseña"] == contraseña:
            
            privilegios = user_info["privilegios"] 
            token = self.generate_token(nombre_usuario, contraseña)
            
            return privilegios, token
            
        else:
            print("Contraseña incorrecta")
            return -1
        
    def login_with_token(self, token):
        if(self.db.exists(token)):
            user_info = pickle.loads(self.db.get(token))
            
            return self.login(user_info["nombre_usuario"], user_info["contraseña"])       

        else:
            return -1
        
    def logout(self, token):
        self.db.delete(token)
        print("Sesión cerrada correctamente")
        
    def edit_user_info(self, nombre_usuario, nombre_completo=None, contraseña=None, privilegios=None):
        if(self.db.hexists("usuarios", nombre_usuario)):
            user_info = pickle.loads(self.db.hget("usuarios", nombre_usuario))
            
            if(nombre_completo != None):
                user_info["nombre_completo"] = nombre_completo
                
            if(contraseña != None):
                user_info["contraseña"] = contraseña
            
            if(privilegios != None):
                user_info["privilegios"] = privilegios
            
            self.db.hset("usuarios", nombre_usuario, pickle.dumps(user_info))
            print("Información de usuario actualizada correctamente")
        else:
            raise("El usuario no existe")
        
# Comprobar que todo funciona correctamente        

manager = RedisManager()

manager.db.flushall() # Borrar todos los datos de la base de datos

manager.register("antonio", "Antonio Cabrera", "1234", 1) # Registrar un usuario

print("\nLogin con usuario y contraseña: ")

privilegios,token = manager.login_and_generate_token("antonio", "1234") # Logearse con el usuario

print("Token: ", token)
print("Privilegios: ", privilegios)


print("\nEditar privilegios del usuario: ")
manager.edit_user_info("antonio", privilegios=2) # Editar privilegios del usuario

print("\nLogin con token: ")

privilegios = manager.login_with_token(token) # Logearse con el token

print("Privilegios: ", privilegios)

print("\nCerrar sesión: ")
manager.logout(token) # Cerrar sesión