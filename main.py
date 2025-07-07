
import os
import json
import logging
import hashlib
import sqlite3
import os

def listar_contenido_recursivo(ruta_base):
    """
    Lista recursivamente todas las carpetas, subcarpetas y archivos
    dentro de una ruta base dada, y retorna la información en dos listas.

    Args:
        ruta_base (str): La ruta del directorio desde donde empezar a listar.

    Returns:
        tuple: Una tupla que contiene dos listas:
               - lista_archivos (list): Lista de rutas completas de todos los archivos.
               - lista_carpetas_vacias (list): Lista de rutas completas de las carpetas vacías.
    """
    lista_archivos = []
    lista_carpetas_vacias = []

    # Verificar si la ruta existe y es un directorio

    if not os.path.isdir(ruta_base):
        print(f"Error: La ruta '{ruta_base}' no es un directorio válido o no existe.")
        return [], []

    # os.walk() genera una tupla (dirpath, dirnames, filenames) para cada directorio
    # dirpath: La ruta del directorio actual que estamos procesando
    # dirnames: Una lista de nombres de subdirectorios en el dirpath actual
    # filenames: Una lista de nombres de archivos en el dirpath actual
    for dirpath, dirnames, filenames in os.walk(ruta_base):
        # Añadir archivos a la lista_archivos
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            lista_archivos.append(ruta_completa_archivo)

        # Comprobar si la carpeta actual está vacía (no contiene archivos ni subdirectorios)
        # Excluimos la ruta_base inicial si no está vacía, ya que generalmente no se considera
        # una "subcarpeta vacía" en el contexto de un listado.
        if not dirnames and not filenames and dirpath != ruta_base:
            lista_carpetas_vacias.append(dirpath)

    return lista_archivos, lista_carpetas_vacias

def generar_hash_archivo(ruta_archivo, chunk_size=4096):
    """
    Genera un hash del contenido de un archivo usando el algoritmo especificado.
    Lee el archivo en bloques para manejar archivos grandes eficientemente.

    Args:
        ruta_archivo (str): La ruta del archivo al que se le calculará el hash.
        chunk_size (int): Tamaño de los bloques a leer del archivo (en bytes).

    Returns:
        str: El hash hexadecimal del contenido del archivo, o None si hay un error.
    """
    hasher = hashlib.sha256()
    
    try:
        with open(ruta_archivo, 'rb') as f: # Open in binary read mode
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break # End of file
                hasher.update(chunk)
        return hasher.hexdigest()
    except IOError as e:
        print(f"Error de lectura del archivo '{ruta_archivo}': {e}")
        return None


# --- Configuración de la base de datos ---
DB_NAME = "backup_metadata.db"
TABLE_NAME = "archivos_respaldados"

def crear_conexion():
    """
    Crea y retorna una conexión a la base de datos SQLite.
    Pregunta al usuario si desea sobrescribir la BD si ya existe.
    """
    if os.path.exists(DB_NAME):
        while True:
            respuesta = input(f"La base de datos '{DB_NAME}' ya existe. ¿Desea sobrescribirla? (s/n): ").lower()
            if respuesta == 's':
                os.remove(DB_NAME)
                print(f"Base de datos '{DB_NAME}' existente eliminada.")
                break
            elif respuesta == 'n':
                print(f"Usando la base de datos existente '{DB_NAME}'.")
                break
            else:
                print("Respuesta no válida. Por favor, ingrese 's' o 'n'.")

    try:
        conn = sqlite3.connect(DB_NAME)
        return conn
    except sqlite3.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def crear_tabla(conn):
    """Crea la tabla de archivos_respaldados si no existe."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                UBICACION TEXT UNIQUE NOT NULL,
                FECHA_MOD REAL NOT NULL, -- Usaremos un timestamp (número real)
                PESO INTEGER NOT NULL,  -- Tamaño del archivo en bytes
                HASH TEXT NOT NULL      -- Hash SHA256 del archivo
            );
        """)
        conn.commit()
        print(f"Tabla '{TABLE_NAME}' creada o ya existente.")
    except sqlite3.Error as e:
        print(f"Error al crear la tabla: {e}")

def insertar_o_actualizar_archivo(conn, ubicacion, fecha_mod, peso, hash_valor):
    """
    Inserta un nuevo registro o actualiza uno existente si la UBICACION ya existe.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {TABLE_NAME} (UBICACION, FECHA_MOD, PESO, HASH)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(UBICACION) DO UPDATE SET
                FECHA_MOD = EXCLUDED.FECHA_MOD,
                PESO = EXCLUDED.PESO,
                HASH = EXCLUDED.HASH;
        """, (ubicacion, fecha_mod, peso, hash_valor))
        conn.commit()
        print(f"Archivo '{ubicacion}' guardado/actualizado correctamente.")
    except sqlite3.Error as e:
        print(f"Error al guardar/actualizar archivo '{ubicacion}': {e}")

if __name__ == '__main__':
    with open("config.json", "r") as file:
        config_ubicacion = json.load(file)
    #establecer las ubicaciones
    #     arranca como indice 
    """
        para las ubicaciones de red no mapeadas // y luego usar las \\ 
    """
    raiz = config_ubicacion[0]['origen']
    print(raiz)