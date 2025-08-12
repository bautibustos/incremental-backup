import os
import json
import logging
import time
import zipfile
import datetime
import concurrent.futures # Importar para paralelismo
import threading # Para obtener el nombre del hilo actual

# --- Configuración de Logging ---
# NO usar basicConfig aquí para permitir logs dinámicos por ejecución
# El logger raíz se configurará dinámicamente en run_incremental_backup_process
# o usará la configuración de main.py si no se añade un handler específico.

LOGS_DIR = "logs" # Carpeta para todos los logs

def setup_dynamic_file_logger(log_file_path):
    """Configura y añade un FileHandler al logger raíz para un archivo de log específico."""
    os.makedirs(LOGS_DIR, exist_ok=True) # Asegura que la carpeta 'logs' exista
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - (%(threadName)s) - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Añadir el handler al logger raíz
    logging.getLogger().addHandler(file_handler)
    return file_handler

def teardown_dynamic_file_logger(handler):
    """Remueve y cierra un FileHandler del logger raíz."""
    logging.getLogger().removeHandler(handler)
    handler.close()

# --- Archivo para la última fecha de backup ---
LAST_BACKUP_DATE_FILE = "last_backup_date.txt"
DATE_FORMAT = "%d-%m-%Y_%H-%M"

# --- Configuración de paralelismo ---
MAX_WORKERS = 3 # Máximo de hilos concurrentes

def leer_ultima_fecha_backup():
    """
    Lee el timestamp numérico de la última fecha de backup desde un archivo.
    Retorna 0.0 si el archivo no existe o está vacío.
    """
    if not os.path.exists(LAST_BACKUP_DATE_FILE):
        logging.info(f"Log de fecha: Archivo '{LAST_BACKUP_DATE_FILE}' no encontrado. Asumiendo backup inicial.")
        return 0.0
    try:
        with open(LAST_BACKUP_DATE_FILE, "r") as f:
            timestamp_str = f.read().strip()
            if timestamp_str:
                return float(timestamp_str)
            else:
                logging.info(f"Log de fecha: Archivo '{LAST_BACKUP_DATE_FILE}' vacío. Asumiendo backup inicial.")
                return 0.0
    except ValueError as e:
        logging.error(f"Log de fecha: Error al leer timestamp de '{LAST_BACKUP_DATE_FILE}': {e}. Asumiendo backup inicial.")
        return 0.0
    except Exception as e:
        logging.error(f"Log de fecha: Error inesperado al leer '{LAST_BACKUP_DATE_FILE}': {e}. Asumiendo backup inicial.")
        return 0.0

def escribir_ultima_fecha_backup(timestamp):
    """
    Escribe el timestamp numérico de la última fecha de backup en un archivo.
    """
    try:
        with open(LAST_BACKUP_DATE_FILE, "w") as f:
            f.write(str(timestamp))
        formatted_date_str = datetime.datetime.fromtimestamp(timestamp).strftime(DATE_FORMAT)
        logging.info(f"Log de fecha: Última fecha de backup ({formatted_date_str}) escrita en '{LAST_BACKUP_DATE_FILE}'.")
    except Exception as e:
        logging.error(f"Log de fecha: Error al escribir en '{LAST_BACKUP_DATE_FILE}': {e}")

def normalizar_ruta_larga_windows(ruta):
    """
    Normaliza una ruta para Windows añadiendo el prefijo \\?\ si es necesario
    para soportar rutas largas (más de 260 caracteres).
    No aplica si la ruta ya es una ruta UNC de red o una ruta extendida.
    """
    if os.name == 'nt': # Solo para Windows
        # Si la ruta ya es una ruta UNC de red (\\server\share) o ya tiene el prefijo extendido,
        # o si es una ruta relativa, no se modifica.
        if ruta.startswith('\\\\?\\') or ruta.startswith('\\\\'):
            return ruta
        # Para rutas de unidad (C:\...)
        if len(ruta) > 259 and (ruta[1] == ':' or ruta.startswith('/')): # Considera también rutas con / en Windows
            return '\\\\?\\' + ruta.replace('/', '\\')
    return ruta

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
    # Normalizar la ruta base para manejar rutas largas en Windows
    ruta_base_normalizada = normalizar_ruta_larga_windows(ruta_base)

    logging.info(f"FS: Iniciando listado recursivo en '{ruta_base_normalizada}'.")
    lista_archivos = []
    lista_carpetas_vacias = []

    if not os.path.isdir(ruta_base):
        logging.error(f"FS: Error: La ruta '{ruta_base}' no es un directorio válido o no existe.")
        return [], []

    for dirpath, dirnames, filenames in os.walk(ruta_base_normalizada):
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            # Normalizar la ruta completa del archivo también
            lista_archivos.append(normalizar_ruta_larga_windows(ruta_completa_archivo))

        if not dirnames and not filenames and dirpath != ruta_base_normalizada:
            # Normalizar la ruta de la carpeta vacía también
            lista_carpetas_vacias.append(normalizar_ruta_larga_windows(dirpath))
    
    logging.info(f"FS: Listado recursivo en '{ruta_base_normalizada}' completado. Encontrados {len(lista_archivos)} archivos.")
    return lista_archivos, lista_carpetas_vacias

def crear_zip_incremental(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip):
    """
    Crea un archivo ZIP con los archivos modificados, manteniendo su estructura de directorios relativa
    al origen. El nombre del ZIP incluye la fecha y hora.

    Args:
        origen_ruta (str): La ruta base de donde provienen los archivos.
        destino_ruta (str): La ruta donde se guardará el archivo ZIP.
        nombre_base_zip (str): El nombre base del archivo ZIP (sin fecha/hora ni extensión).
        files_to_zip (list): Una lista de rutas completas de los archivos a incluir en el ZIP.
    """
    if not files_to_zip:
        logging.info(f"ZIP: No hay archivos para zipear en el origen '{origen_ruta}'.")
        return

    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"INC_{nombre_base_zip}_{timestamp_str}.zip"
    full_zip_path = os.path.join(destino_ruta, zip_filename)

    os.makedirs(destino_ruta, exist_ok=True)

    logging.info(f"ZIP: Creando archivo ZIP en '{full_zip_path}' con {len(files_to_zip)} archivos.")

    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    arcname = os.path.relpath(file_path, origen_ruta)
                    zipf.write(file_path, arcname)
                    logging.debug(f"ZIP: Añadido '{file_path}' como '{arcname}' al ZIP.")
                except FileNotFoundError:
                    logging.warning(f"ZIP: Error: Archivo '{file_path}' no encontrado al intentar añadirlo al ZIP. Puede haber sido movido o eliminado.")
                except PermissionError:
                    logging.error(f"ZIP: Error: Permiso denegado para leer el archivo '{file_path}' al intentar añadirlo al ZIP.")
                except Exception as e:
                    logging.error(f"ZIP: Error inesperado al añadir archivo '{file_path}' al ZIP: {e}")
        logging.info(f"ZIP: Archivo ZIP '{zip_filename}' creado exitosamente.")
    except Exception as e:
        logging.error(f"ZIP: Error al crear el archivo ZIP '{full_zip_path}': {e}")


# --- Función principal de Backup Incremental (ejecutada por hilo) ---
def ejecutar_backup_incremental(origen_ruta, destino_ruta, nombre_base_zip, last_backup_timestamp):
    """
    Ejecuta el proceso de backup incremental para una ruta de origen dada,
    basado en la fecha de la última ejecución, y zipea los archivos modificados.
    """
    start_time = time.time()
    logging.info(f"PROCESO: Iniciando backup incremental para el origen: '{origen_ruta}'")
    
    archivos_en_origen, _ = listar_contenido_recursivo(origen_ruta)
    
    files_to_zip = []
    archivos_modificados_o_nuevos = 0
    archivos_sin_cambios = 0
    archivos_error = 0

    for ruta_archivo in archivos_en_origen:
        try:
            stats = os.stat(ruta_archivo)
            fecha_mod_actual = stats.st_mtime

            if fecha_mod_actual > last_backup_timestamp:
                # logging.info(f"PROCESO: '{ruta_archivo}' modificado/nuevo desde el último backup. Añadiendo a la lista de zipeo.") # Suprimido
                files_to_zip.append(ruta_archivo)
                archivos_modificados_o_nuevos += 1
            else:
                # logging.debug(f"PROCESO: '{ruta_archivo}' sin cambios desde el último backup.") # Suprimido
                archivos_sin_cambios += 1

        except FileNotFoundError:
            logging.warning(f"PROCESO: Archivo '{ruta_archivo}' no encontrado durante el procesamiento (posiblemente eliminado).")
            archivos_error += 1
        except PermissionError:
            logging.error(f"PROCESO: Permiso denegado para acceder a '{ruta_archivo}'.")
            archivos_error += 1
        except Exception as e:
            logging.error(f"PROCESO: Error inesperado al procesar '{ruta_archivo}': {e}")
            archivos_error += 1
            
    crear_zip_incremental(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip)

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"PROCESO: Backup incremental para '{origen_ruta}' completado en {duration:.2f} segundos. Errores: {archivos_error}")
    return archivos_error


# --- Función para ejecutar el proceso de backup incremental (con paralelismo) ---
def run_incremental_backup_process(config_data):
    """
    Ejecuta el proceso de backup incremental para todas las ubicaciones de origen
    definidas en los datos de configuración, utilizando paralelismo.
    """
    logging.info("INICIO: Aplicación de Backup Incremental (Simplificada con Zipeo - Paralelo).")
    logging.debug(f"Configuración recibida en run_incremental_backup_process: {json.dumps(config_data, indent=2)}")

    total_backup_errors = 0

        last_backup_timestamp_global = leer_ultima_fecha_backup()
        formatted_last_backup_date = datetime.datetime.fromtimestamp(last_backup_timestamp_global).strftime(DATE_FORMAT)
        logging.info(f"Último backup registrado: {formatted_last_backup_date}")

    if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
        config_ubicaciones = config_data['origenes']
        if all(key in d for d in config_ubicaciones for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
            
            logging.info(f"PROCESO: Se iniciarán {len(config_ubicaciones)} tareas de backup incremental en paralelo (máx. {MAX_WORKERS} hilos).")
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_origin = {
                    executor.submit(ejecutar_backup_incremental, entry['origen_ruta'], entry['destino_ruta'], entry['nombre_base_zip'], last_backup_timestamp_global): entry
                    for entry in config_ubicaciones
                }

                for future in concurrent.futures.as_completed(future_to_origin):
                    origin_entry = future_to_origin[future]
                    origin_path_for_log = origin_entry.get('origen_ruta', 'N/A')
                    try:
                        errors = future.result()
                        total_backup_errors += errors
                        logging.info(f"PROCESO: Tarea de backup incremental para '{origin_path_for_log}' finalizada con {errors} errores.")
                    except Exception as exc:
                        logging.error(f"PROCESO: La tarea de backup incremental para '{origin_path_for_log}' generó una excepción: {exc}")
                        total_backup_errors += 1
            
        else:
            logging.critical("ERROR: El formato de la clave 'origenes' en config.json no es el esperado. Debe ser una lista de objetos con las claves 'origen_ruta', 'destino_ruta' y 'nombre_base_zip'.")
            total_backup_errors += 1
    else:
        logging.critical("ERROR: El formato del archivo config.json no es el esperado. Debe ser un objeto con la clave 'origenes' (lista).")
        total_backup_errors += 1

        logging.info(f"FIN: Aplicación de Backup Incremental (Simplificada con Zipeo - Paralelo) finalizada con {total_backup_errors} errores totales en el proceso de backup.")
        
        escribir_ultima_fecha_backup(time.time())

    return total_backup_errors

# El bloque principal de ejecución (if __name__ == '__main__':) se mantiene para pruebas directas
if __name__ == '__main__':
    config_file_path = "config.json"
    
    try:
        with open(config_file_path, "r") as file:
            config_data = json.load(file)
        logging.info(f"Configuración cargada desde '{config_file_path}'.")
    except FileNotFoundError:
        logging.warning(f"Archivo de configuración '{config_file_path}' no encontrado. Usando configuración de ejemplo.")
    except json.JSONDecodeError as e:
        logging.critical(f"ERROR: Error al parsear '{config_file_path}'. Verifique el formato JSON: {e}. Usando configuración de ejemplo.")
    except Exception as e:
        logging.critical(f"ERROR: Error inesperado al cargar la configuración: {e}. Usando configuración de ejemplo.")

    run_incremental_backup_process(config_data)
