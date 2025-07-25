import os
import json
import logging
import time
import zipfile
import datetime
import concurrent.futures # Importar para paralelismo
import threading # Para obtener el nombre del hilo actual

# Importa la función para escribir la última fecha de backup del script incremental
# Esto permite que el backup completo también actualice el punto de referencia para incrementales
from incremental import escribir_ultima_fecha_backup, LAST_BACKUP_DATE_FILE, DATE_FORMAT

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - (%(threadName)s) - %(message)s', # Añadido %(threadName)s
    handlers=[
        logging.FileHandler("backup_log.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# DATE_FORMAT ya se importa de incremental.py

# --- Configuración de paralelismo ---
MAX_WORKERS = 3 # Máximo de hilos concurrentes

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
    logging.info(f"FS: Iniciando listado recursivo en '{ruta_base}'.")
    lista_archivos = []
    lista_carpetas_vacias = []

    if not os.path.isdir(ruta_base):
        logging.error(f"FS: Error: La ruta '{ruta_base}' no es un directorio válido o no existe. No se puede listar el contenido.")
        return [], []

    for dirpath, dirnames, filenames in os.walk(ruta_base):
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            lista_archivos.append(ruta_completa_archivo)

        if not dirnames and not filenames and dirpath != ruta_base:
            lista_carpetas_vacias.append(dirpath)
    
    logging.info(f"FS: Listado recursivo en '{ruta_base}' completado. Encontrados {len(lista_archivos)} archivos.")
    return lista_archivos, lista_carpetas_vacias

def crear_zip_completo(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip, empty_folders_to_add=None):
    """
    Crea un archivo ZIP con todos los archivos y opcionalmente carpetas vacías,
    manteniendo su estructura de directorios relativa al origen.
    El nombre del ZIP incluye la fecha y hora.

    Args:
        origen_ruta (str): La ruta base de donde provienen los archivos.
        destino_ruta (str): La ruta donde se guardará el archivo ZIP.
        nombre_base_zip (str): El nombre base del archivo ZIP (sin fecha/hora ni extensión).
        files_to_zip (list): Una lista de rutas completas de los archivos a incluir en el ZIP.
        empty_folders_to_add (list, optional): Lista de rutas completas de carpetas vacías a añadir.
                                               Por defecto es None.

    Returns:
        int: El número de errores encontrados al añadir archivos/carpetas al ZIP.
    """
    zip_errors_count = 0

    if not files_to_zip and not (empty_folders_to_add and len(empty_folders_to_add) > 0):
        logging.info(f"ZIP: No hay archivos ni carpetas vacías para zipear en el origen '{origen_ruta}'.")
        return zip_errors_count

    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"COM_{nombre_base_zip}_{timestamp_str}.zip"
    full_zip_path = os.path.join(destino_ruta, zip_filename)

    os.makedirs(destino_ruta, exist_ok=True)

    logging.info(f"ZIP: Creando archivo ZIP en '{full_zip_path}' con {len(files_to_zip)} archivos y {len(empty_folders_to_add) if empty_folders_to_add else 0} carpetas vacías.")

    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    arcname = os.path.relpath(file_path, origen_ruta)
                    zipf.write(file_path, arcname)
                    logging.debug(f"ZIP: Añadido archivo '{file_path}' como '{arcname}' al ZIP.")
                except FileNotFoundError:
                    logging.warning(f"ZIP: Error: Archivo '{file_path}' no encontrado al intentar añadirlo al ZIP. Puede haber sido movido o eliminado.")
                    zip_errors_count += 1
                except PermissionError:
                    logging.error(f"ZIP: Error: Permiso denegado para leer el archivo '{file_path}' al intentar añadirlo al ZIP.")
                    zip_errors_count += 1
                except Exception as e:
                    logging.error(f"ZIP: Error inesperado al añadir archivo '{file_path}' al ZIP: {e}")
                    zip_errors_count += 1
            
            if empty_folders_to_add:
                for folder_path in empty_folders_to_add:
                    try:
                        arcname_folder = os.path.relpath(folder_path, origen_ruta) + '/'
                        zip_info = zipfile.ZipInfo(arcname_folder)
                        zip_info.external_attr = 0o40775 << 16 
                        zipf.writestr(zip_info, '')
                        logging.debug(f"ZIP: Añadida carpeta vacía '{folder_path}' como '{arcname_folder}' al ZIP.")
                    except Exception as e:
                        logging.error(f"ZIP: Error inesperado al añadir carpeta vacía '{folder_path}' al ZIP: {e}")
                        zip_errors_count += 1

        if zip_errors_count == 0:
            logging.info(f"ZIP: Archivo ZIP '{zip_filename}' creado exitosamente.")
        else:
            logging.warning(f"ZIP: Archivo ZIP '{zip_filename}' creado con {zip_errors_count} errores de archivo/carpeta.")
    except Exception as e:
        logging.error(f"ZIP: Error crítico al iniciar la creación del archivo ZIP '{full_zip_path}': {e}")
        zip_errors_count += len(files_to_zip) + (len(empty_folders_to_add) if empty_folders_to_add else 0)
    
    return zip_errors_count


# --- Función principal de Backup Completo (ejecutada por hilo) ---
def ejecutar_backup_completo(origen_ruta, destino_ruta, nombre_base_zip):
    """
    Ejecuta el proceso de backup completo para una ruta de origen dada,
    zipeando todos los archivos y carpetas vacías encontrados.
    """
    start_time = time.time()
    logging.info(f"PROCESO: Iniciando backup completo para el origen: '{origen_ruta}'")
    
    archivos_en_origen, carpetas_vacias_en_origen = listar_contenido_recursivo(origen_ruta)
    
    # --- DIAGNOSTICO ---
    logging.info(f"DIAGNOSTICO: Archivos encontrados por el listado: {len(archivos_en_origen)}")
    if len(archivos_en_origen) > 0 and len(archivos_en_origen) < 10:
        logging.info(f"DIAGNOSTICO: Lista de archivos (todos): {archivos_en_origen}")
    elif len(archivos_en_origen) >= 10:
        logging.info(f"DIAGNOSTICO: Primeros 10 archivos: {archivos_en_origen[:10]}")
    else:
        logging.info(f"DIAGNOSTICO: No se encontraron archivos en '{origen_ruta}'.")

    logging.info(f"DIAGNOSTICO: Carpetas vacías encontradas: {len(carpetas_vacias_en_origen)}")
    if len(carpetas_vacias_en_origen) > 0 and len(carpetas_vacias_en_origen) < 10:
        logging.info(f"DIAGNOSTICO: Lista de carpetas vacías (todas): {carpetas_vacias_en_origen}")
    elif len(carpetas_vacias_en_origen) >= 10:
        logging.info(f"DIAGNOSTICO: Primeras 10 carpetas vacías: {carpetas_vacias_en_origen[:10]}")
    else:
        logging.info(f"DIAGNOSTICO: No se encontraron carpetas vacías en '{origen_ruta}'.")
    # --- FIN DIAGNOSTICO ---

    files_to_zip = []
    archivos_procesados = 0
    archivos_error_listado = 0

    for ruta_archivo in archivos_en_origen:
        try:
            files_to_zip.append(ruta_archivo)
            archivos_procesados += 1
            logging.debug(f"PROCESO: Añadiendo '{ruta_archivo}' a la lista de zipeo.")

        except FileNotFoundError:
            logging.warning(f"PROCESO: Archivo '{ruta_archivo}' no encontrado durante el procesamiento (posiblemente eliminado).")
            archivos_error_listado += 1
        except PermissionError:
            logging.error(f"PROCESO: Permiso denegado para acceder a '{ruta_archivo}'.")
            archivos_error_listado += 1
        except Exception as e:
            logging.error(f"PROCESO: Error inesperado al procesar '{ruta_archivo}': {e}")
            archivos_error_listado += 1
            
    zip_creation_errors = crear_zip_completo(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip, carpetas_vacias_en_origen)
    
    total_errors = archivos_error_listado + zip_creation_errors

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"PROCESO: Backup completo para '{origen_ruta}' completado en {duration:.2f} segundos. Errores: {total_errors}")
    return total_errors # Return total errors for the scheduler


# --- Función para ejecutar el proceso de backup completo (con paralelismo) ---
def run_full_backup_process(config_data):
    """
    Ejecuta el proceso de backup completo para todas las ubicaciones de origen
    definidas en los datos de configuración, utilizando paralelismo.
    """
    logging.info("INICIO: Aplicación de Backup Completo con Zipeo (Paralelo).")
    logging.debug(f"Configuración recibida en run_full_backup_process: {json.dumps(config_data, indent=2)}")
    
    total_backup_errors = 0

    if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
        config_ubicaciones = config_data['origenes']
        if all(key in d for d in config_ubicaciones for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
            
            logging.info(f"PROCESO: Se iniciarán {len(config_ubicaciones)} tareas de backup completo en paralelo (máx. {MAX_WORKERS} hilos).")
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_origin = {
                    executor.submit(ejecutar_backup_completo, entry['origen_ruta'], entry['destino_ruta'], entry['nombre_base_zip']): entry
                    for entry in config_ubicaciones
                }

                for future in concurrent.futures.as_completed(future_to_origin):
                    origin_entry = future_to_origin[future]
                    origin_path_for_log = origin_entry.get('origen_ruta', 'N/A')
                    try:
                        errors = future.result()
                        total_backup_errors += errors
                        logging.info(f"PROCESO: Tarea de backup completo para '{origin_path_for_log}' finalizada con {errors} errores.")
                    except Exception as exc:
                        logging.error(f"PROCESO: La tarea de backup completo para '{origin_path_for_log}' generó una excepción: {exc}")
                        total_backup_errors += 1
            
        else:
            logging.critical("ERROR: El formato de la clave 'origenes' en config.json no es el esperado. Debe ser una lista de objetos con las claves 'origen_ruta', 'destino_ruta' y 'nombre_base_zip'.")
            total_backup_errors += 1
    else:
        logging.critical("ERROR: El formato del archivo config.json no es el esperado. Debe ser un objeto con la clave 'origenes' (lista).")
        total_backup_errors += 1

    logging.info(f"FIN: Aplicación de Backup Completo con Zipeo finalizada con {total_backup_errors} errores totales en el proceso de backup.")
    
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
        
    run_full_backup_process(config_data)
