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
        
    # Funciones Help Desk
    
    # Función de petición de ayuda con prioridad
    def create_ticket(self, nombre_usuario, titulo, descripcion, prioridad):
        if(self.db.hexists("usuarios", nombre_usuario)): # Ver si existe el usuario
            ticket_info = {"titulo": titulo, "descripcion": descripcion, "usuario": nombre_usuario}
            
            self.db.zadd("tickets", {pickle.dumps(ticket_info): prioridad})
            
            print("Ticket creado correctamente")
        else:
            raise("El usuario no existe")

    # Función de atención a usuarios
    def attend_ticket(self):
        
        # Si no hay tickets se queda en espera
        
        if(self.db.zcard("tickets") == 0):
            print("No quedan tickets por atender, esperando...")

            while(self.db.zcard("tickets") == 0):
                pass
        
        # Se atienden primero los tickets con mayor valor de prioridad

        ticket = self.db.zrevrange("tickets", 0, 0)[0]
                
        ticket = pickle.loads(ticket)
        
        self.db.zrem("tickets", pickle.dumps(ticket))
                
        id_usuario = ticket["usuario"]
        
        print("Ticket:" + ticket["titulo"] + ", atendido correctamente")
        
        return id_usuario
        
        
    # Funciones extra    
    
    def get_user_info(self, nombre_usuario):
        if(self.db.hexists("usuarios", nombre_usuario)):
            user_info = pickle.loads(self.db.hget("usuarios", nombre_usuario))
            return user_info
        else:
            raise("El usuario no existe")
        
    def get_all_users(self):
        users = self.db.hgetall("usuarios")
        
        for user in users:
            print(user.decode("utf-8"), pickle.loads(users[user]))
        
    def logout(self, token):
        self.db.delete(token)
        print("Sesión cerrada correctamente")
        
    def delete_user(self, nombre_usuario):
        if(self.db.hexists("usuarios", nombre_usuario)):
            self.db.hdel("usuarios", nombre_usuario)
            print("Usuario eliminado correctamente")
        else:
            raise("El usuario no existe")
        
# Test       

manager = RedisManager()

manager.db.flushall() # Borrar todos los datos de la base de datos

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

print("\nHelp Desk: ")

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