import os
import json
import logging
import hashlib
import sqlite3
import time

# --- Configuración de Logging ---
# Configura el logger para guardar mensajes en un archivo y mostrarlos en la consola
logging.basicConfig(
    level=logging.INFO, # Nivel mínimo de mensajes a registrar (INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backup_log.log", encoding='utf-8'), # Guarda los logs en un archivo
        logging.StreamHandler() # Muestra los logs en la consola
    ]
)

# --- Configuración de la base de datos ---
DB_NAME = "backup_metadata.db"
TABLE_NAME = "archivos_respaldados"

def crear_conexion():
    """
    Crea y retorna una conexión a la base de datos SQLite.
    La crea si no existe, o se conecta a la existente.
    """
    try:
        conn = sqlite3.connect(DB_NAME)
        logging.info(f"Conexión a la base de datos '{DB_NAME}' establecida.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error al conectar a la base de datos '{DB_NAME}': {e}")
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
        logging.info(f"Tabla '{TABLE_NAME}' creada o ya existente.")
    except sqlite3.Error as e:
        logging.error(f"Error al crear la tabla '{TABLE_NAME}': {e}")

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
        logging.info(f"DB: Archivo '{ubicacion}' guardado/actualizado correctamente.")
    except sqlite3.Error as e:
        logging.error(f"DB: Error al guardar/actualizar archivo '{ubicacion}': {e}")

def obtener_info_archivo(conn, ubicacion):
    """Obtiene la información de un archivo de la base de datos por su UBICACION."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT UBICACION, FECHA_MOD, PESO, HASH FROM {TABLE_NAME} WHERE UBICACION = ?;", (ubicacion,))
        return cursor.fetchone() # Retorna una tupla o None si no se encuentra
    except sqlite3.Error as e:
        logging.error(f"DB: Error al obtener información del archivo '{ubicacion}': {e}")
        return None

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

    logging.info(f"FS: Iniciando listado recursivo en '{ruta_base}'.")
    if not os.path.isdir(ruta_base):
        logging.error(f"FS: Error: La ruta '{ruta_base}' no es un directorio válido o no existe.")
        return [], []

    for dirpath, dirnames, filenames in os.walk(ruta_base):
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            lista_archivos.append(ruta_completa_archivo)

        if not dirnames and not filenames and dirpath != ruta_base:
            lista_carpetas_vacias.append(dirpath)
    
    logging.info(f"FS: Listado recursivo en '{ruta_base}' completado. Encontrados {len(lista_archivos)} archivos.")
    return lista_archivos, lista_carpetas_vacias

def generar_hash_archivo(ruta_archivo, chunk_size=4096):
    """
    Genera un hash SHA256 del contenido de un archivo.
    Lee el archivo en bloques para manejar archivos grandes eficientemente.

    Args:
        ruta_archivo (str): La ruta del archivo al que se le calculará el hash.
        chunk_size (int): Tamaño de los bloques a leer del archivo (en bytes).

    Returns:
        str: El hash hexadecimal del contenido del archivo, o None si hay un error.
    """
    hasher = hashlib.sha256()
    
    try:
        with open(ruta_archivo, 'rb') as f: # Abrir en modo binario de lectura
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break # Fin del archivo
                hasher.update(chunk)
        return hasher.hexdigest()
    except IOError as e:
        logging.error(f"HASH: Error de lectura del archivo '{ruta_archivo}': {e}")
        return None
    except Exception as e:
        logging.error(f"HASH: Error inesperado al calcular hash de '{ruta_archivo}': {e}")
        return None

# --- Función principal de Backup Incremental ---
def ejecutar_backup_incremental(conn, origen_path):
    """
    Ejecuta el proceso de backup incremental para una ruta de origen dada.
    """
    logging.info(f"PROCESO: Iniciando backup incremental para el origen: '{origen_path}'")
    
    archivos_en_origen, _ = listar_contenido_recursivo(origen_path)
    archivos_modificados_o_nuevos = 0
    archivos_sin_cambios = 0
    archivos_error = 0

    for ruta_archivo in archivos_en_origen:
        print("aaaa",ruta_archivo)
        try:
            # Obtener metadatos actuales del archivo
            stats = os.stat(ruta_archivo)
            fecha_mod_actual = stats.st_mtime
            peso_actual = stats.st_size

            # Obtener información del archivo desde la DB
            info_db = obtener_info_archivo(conn, ruta_archivo)

            if info_db:
                # El archivo ya existe en la DB, verificar si ha cambiado
                fecha_mod_db = info_db[1]
                peso_db = info_db[2]
                hash_db = info_db[3]

                if fecha_mod_actual == fecha_mod_db and peso_actual == peso_db:
                    # Si fecha y peso son iguales, asumimos que no hay cambios y evitamos calcular hash
                    logging.debug(f"PROCESO: '{ruta_archivo}' sin cambios detectados por metadatos. Saltando hash.")
                    archivos_sin_cambios += 1
                else:
                    # Fecha o peso han cambiado, calcular hash para verificación definitiva
                    logging.info(f"PROCESO: '{ruta_archivo}' - Metadatos cambiaron. Calculando hash para verificar.")
                    hash_actual = generar_hash_archivo(ruta_archivo)
                    if hash_actual and hash_actual != hash_db:
                        # El hash es diferente, el archivo ha sido modificado
                        insertar_o_actualizar_archivo(conn, ruta_archivo, fecha_mod_actual, peso_actual, hash_actual)
                        archivos_modificados_o_nuevos += 1
                        logging.info(f"PROCESO: '{ruta_archivo}' modificado. Actualizado en DB.")
                    elif hash_actual == hash_db:
                        # Los metadatos cambiaron pero el hash es el mismo (caso raro pero posible)
                        logging.warning(f"PROCESO: '{ruta_archivo}' - Metadatos cambiaron pero hash es el mismo. (Posible falsa alarma o error de sistema de archivos).")
                        archivos_sin_cambios += 1
                    else:
                        archivos_error += 1
                        logging.error(f"PROCESO: No se pudo calcular el hash de '{ruta_archivo}'.")
            else:
                # El archivo es nuevo, insertarlo en la DB
                logging.info(f"PROCESO: '{ruta_archivo}' es nuevo. Calculando hash.")
                hash_actual = generar_hash_archivo(ruta_archivo)
                if hash_actual:
                    insertar_o_actualizar_archivo(conn, ruta_archivo, fecha_mod_actual, peso_actual, hash_actual)
                    archivos_modificados_o_nuevos += 1
                    logging.info(f"PROCESO: '{ruta_archivo}' nuevo. Insertado en DB.")
                else:
                    archivos_error += 1
                    logging.error(f"PROCESO: No se pudo calcular el hash de '{ruta_archivo}'.")

        except FileNotFoundError:
            logging.warning(f"PROCESO: Archivo '{ruta_archivo}' no encontrado durante el procesamiento (posiblemente eliminado).")
            archivos_error += 1 # Considerar como error para seguimiento
        except PermissionError:
            logging.error(f"PROCESO: Permiso denegado para acceder a '{ruta_archivo}'.")
            archivos_error += 1
        except Exception as e:
            logging.error(f"PROCESO: Error inesperado al procesar '{ruta_archivo}': {e}")
            archivos_error += 1
            
    logging.info(f"PROCESO: Backup incremental para '{origen_path}' completado.")
    logging.info(f"PROCESO: Resumen - Nuevos/Modificados: {archivos_modificados_o_nuevos}, Sin cambios: {archivos_sin_cambios}, Errores: {archivos_error}")


# --- Bloque Principal de Ejecución ---
if __name__ == '__main__':
    logging.info("INICIO: Aplicación de Backup Incremental.")

    # 1. Cargar configuraciones desde config.json
    config_file_path = "config.json"
    if not os.path.exists(config_file_path):
        logging.critical(f"ERROR: Archivo de configuración '{config_file_path}' no encontrado. Creando uno de ejemplo.")
        # Crear un config.json de ejemplo si no existe
        example_config = [
            {"origen": os.path.join(os.getcwd(), "test_origin_1")},
            {"origen": os.path.join(os.getcwd(), "test_origin_2")}
        ]
        with open(config_file_path, "w") as f:
            json.dump(example_config, f, indent=4)
        logging.info(f"Archivo '{config_file_path}' creado con rutas de ejemplo. Por favor, edítelo con sus rutas reales.")
        
        # Crear directorios de prueba para el ejemplo
        for entry in example_config:
            path = entry['origen']
            if not os.path.exists(path):
                os.makedirs(path)
                logging.info(f"Directorio de prueba '{path}' creado.")
                # Crear un archivo de ejemplo dentro
                with open(os.path.join(path, "example_file.txt"), "w") as f:
                    f.write("This is an example file.")
                with open(os.path.join(path, "another_file.log"), "w") as f:
                    f.write("Log content.")
            else:
                logging.info(f"Directorio de prueba '{path}' ya existe.")


    try:
        with open(config_file_path, "r") as file:
            config_ubicaciones = json.load(file)
        logging.info(f"Configuración cargada desde '{config_file_path}'.")
    except FileNotFoundError:
        logging.critical(f"ERROR: El archivo de configuración '{config_file_path}' no se encontró. Asegúrese de que exista.")
        exit(1)
    except json.JSONDecodeError as e:
        logging.critical(f"ERROR: Error al parsear '{config_file_path}'. Verifique el formato JSON: {e}")
        exit(1)
    except Exception as e:
        logging.critical(f"ERROR: Error inesperado al cargar la configuración: {e}")
        exit(1)

    # 2. Establecer conexión a la base de datos
    conn = crear_conexion()
    if not conn:
        logging.critical("ERROR: No se pudo establecer la conexión a la base de datos. Saliendo.")
        exit(1)
    
    # 3. Crear la tabla si no existe
    crear_tabla(conn)

    # 4. Iterar sobre cada ubicación de origen en el archivo de configuración
    if isinstance(config_ubicaciones, list) and all('origen' in d for d in config_ubicaciones):
        for i, config_entry in enumerate(config_ubicaciones):
            origen_path = config_entry['origen']
            logging.info(f"\n--- Procesando origen [{i+1}/{len(config_ubicaciones)}]: '{origen_path}' ---")
            ejecutar_backup_incremental(conn, origen_path)
    else:
        logging.critical("ERROR: El formato del archivo config.json no es el esperado. Debe ser una lista de objetos con la clave 'origen'.")

    # 5. Cerrar la conexión a la base de datos al finalizar
    if conn:
        conn.close()
        logging.info(f"Conexión a '{DB_NAME}' cerrada.")

    logging.info("FIN: Aplicación de Backup Incremental finalizada.")
