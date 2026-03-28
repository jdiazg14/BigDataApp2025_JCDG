"""
Script de inicialización de la base de datos.

Crea las colecciones necesarias e inserta el usuario administrador inicial
con contraseña encriptada usando werkzeug.security (el mismo mecanismo que
usa la aplicación para verificar el login).

Las credenciales del administrador se leen desde el archivo .env:
    APP_USER_ADMIN           — nombre de usuario (ej. admin)
    APP_USER_ADMIN_PASSWORD  — contraseña en texto plano que se encriptará

Ejecútelo UNA ÚNICA VEZ antes de arrancar la aplicación por primera vez:

    python init_db.py

El script es idempotente: si el usuario ya existe, no realiza cambios.
"""

import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from werkzeug.security import generate_password_hash

# Cargar variables de entorno desde .env
load_dotenv()

MONGO_URI       = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
MONGO_DB        = os.getenv('MONGO_DB', 'bigdataapp')
MONGO_COLECCION = os.getenv('MONGO_COLECCION', 'usuario_roles')

# Credenciales del administrador inicial — definidas en .env, nunca en el código
APP_USER_ADMIN          = os.getenv('APP_USER_ADMIN')
APP_USER_ADMIN_PASSWORD = os.getenv('APP_USER_ADMIN_PASSWORD')


def main() -> None:
    # Validar que las credenciales estén definidas en el entorno
    if not APP_USER_ADMIN or not APP_USER_ADMIN_PASSWORD:
        print("[init_db] ERROR: Las variables APP_USER_ADMIN y APP_USER_ADMIN_PASSWORD")
        print("          deben estar definidas en el archivo .env antes de ejecutar este script.")
        sys.exit(1)

    print(f"[init_db] Conectando a MongoDB  →  {MONGO_URI} | DB: {MONGO_DB}")

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
    except ConnectionFailure as exc:
        print(f"[init_db] ERROR: No se pudo conectar a MongoDB: {exc}")
        print("         Verifique que el servicio esté activo: Get-Service -Name MongoDB")
        sys.exit(1)

    db        = client[MONGO_DB]
    coleccion = db[MONGO_COLECCION]

    # Idempotencia: solo insertar si el usuario no existe ya en la colección
    if coleccion.find_one({'usuario': APP_USER_ADMIN}):
        print(f"[init_db] El usuario '{APP_USER_ADMIN}' ya existe. No se realizaron cambios.")
        client.close()
        return

    # Contraseña encriptada con werkzeug.security (compatible con check_password_hash en login)
    password_hash = generate_password_hash(APP_USER_ADMIN_PASSWORD)

    documento = {
        'usuario': APP_USER_ADMIN,
        'password': password_hash,
        'permisos': {
            'admin_usuarios':     True,
            'admin_elastic':      True,
            'admin_data_elastic': True,
        },
    }

    coleccion.insert_one(documento)

    print(f"[init_db] ✓ Usuario '{APP_USER_ADMIN}' creado en '{MONGO_DB}.{MONGO_COLECCION}'.")
    print( "[init_db]   IMPORTANTE: cambie la contraseña después del primer acceso.")

    client.close()


if __name__ == '__main__':
    main()
