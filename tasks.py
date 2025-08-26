import os
import json
import logging
import time
import zipfile
import datetime
from celery import Celery

# --- Celery Configuration ---
# It's recommended to use a configuration file for production, but for simplicity,
# we'll configure it directly here.
# Replace 'redis://localhost:6379/0' with your broker's URL if it's different.
app = Celery('tasks', broker='redis://localhost:6379/0')

# --- Logging Configuration ---
# It's good practice for tasks to have their own logging.
LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)

# Configure a logger for the tasks
task_log_filename = datetime.datetime.now().strftime("task_runner_%Y%m%d.log")
task_log_filepath = os.path.join(LOGS_DIR, task_log_filename)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - (%(threadName)s) - %(message)s',
    handlers=[
        logging.FileHandler(task_log_filepath, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Constants ---
LAST_BACKUP_DATE_FILE = "last_backup_date.txt"
DATE_FORMAT = "%d-%m-%Y_%H-%M"

# --- Helper Functions (from completo.py and incremental.py) ---

def normalizar_ruta_larga_windows(ruta):
    if os.name == 'nt':
        if ruta.startswith('\\\\?\\') or ruta.startswith('\\\\'):
            return ruta
        if len(ruta) > 259 and (ruta[1] == ':' or ruta.startswith('/')):
            return '\\\\?\\' + ruta.replace('/', '\\')
    return ruta

def listar_contenido_recursivo(ruta_base):
    ruta_base_normalizada = normalizar_ruta_larga_windows(ruta_base)
    logging.info(f"FS: Iniciando listado recursivo en '{ruta_base_normalizada}'.")
    lista_archivos = []
    lista_carpetas_vacias = []
    if not os.path.isdir(ruta_base_normalizada):
        logging.error(f"FS: Error: La ruta '{ruta_base_normalizada}' no es un directorio válido o no existe.")
        return [], []
    for dirpath, dirnames, filenames in os.walk(ruta_base_normalizada):
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            lista_archivos.append(normalizar_ruta_larga_windows(ruta_completa_archivo))
        if not dirnames and not filenames and dirpath != ruta_base_normalizada:
            lista_carpetas_vacias.append(normalizar_ruta_larga_windows(dirpath))
    logging.info(f"FS: Listado en '{ruta_base_normalizada}' completado. {len(lista_archivos)} archivos.")
    return lista_archivos, lista_carpetas_vacias

def crear_zip_completo(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip, empty_folders_to_add=None):
    zip_errors_count = 0
    if not files_to_zip and not (empty_folders_to_add and len(empty_folders_to_add) > 0):
        logging.info(f"ZIP: No hay archivos ni carpetas para zipear en '{origen_ruta}'.")
        return zip_errors_count
    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"COM_{nombre_base_zip}_{timestamp_str}.zip"
    full_zip_path = normalizar_ruta_larga_windows(os.path.join(destino_ruta, zip_filename))
    normalized_destino_ruta = normalizar_ruta_larga_windows(destino_ruta)
    os.makedirs(normalized_destino_ruta, exist_ok=True)
    logging.info(f"ZIP: Creando '{full_zip_path}' con {len(files_to_zip)} archivos y {len(empty_folders_to_add) if empty_folders_to_add else 0} carpetas vacías.")
    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    arcname = os.path.relpath(file_path, origen_ruta.replace('\\\\?\\', ''))
                    zipf.write(file_path, arcname)
                except Exception as e:
                    logging.error(f"ZIP: Error añadiendo archivo '{file_path}': {e}")
                    zip_errors_count += 1
            if empty_folders_to_add:
                for folder_path in empty_folders_to_add:
                    try:
                        arcname_folder = os.path.relpath(folder_path, origen_ruta.replace('\\\\?\\', '')) + '/'
                        zip_info = zipfile.ZipInfo(arcname_folder)
                        zip_info.external_attr = 0o40775 << 16
                        zipf.writestr(zip_info, '')
                    except Exception as e:
                        logging.error(f"ZIP: Error añadiendo carpeta vacía '{folder_path}': {e}")
                        zip_errors_count += 1
        if zip_errors_count == 0:
            logging.info(f"ZIP: Archivo '{zip_filename}' creado exitosamente.")
        else:
            logging.warning(f"ZIP: Archivo '{zip_filename}' creado con {zip_errors_count} errores.")
    except Exception as e:
        logging.error(f"ZIP: Error crítico creando ZIP '{full_zip_path}': {e}")
        zip_errors_count += len(files_to_zip) + (len(empty_folders_to_add) if empty_folders_to_add else 0)
    return zip_errors_count

def crear_zip_incremental(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip):
    if not files_to_zip:
        logging.info(f"ZIP: No hay archivos para zipear en '{origen_ruta}'.")
        return
    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"INC_{nombre_base_zip}_{timestamp_str}.zip"
    full_zip_path = os.path.join(destino_ruta, zip_filename)
    os.makedirs(destino_ruta, exist_ok=True)
    logging.info(f"ZIP: Creando '{full_zip_path}' con {len(files_to_zip)} archivos.")
    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    arcname = os.path.relpath(file_path, origen_ruta)
                    zipf.write(file_path, arcname)
                except Exception as e:
                    logging.error(f"ZIP: Error añadiendo archivo '{file_path}': {e}")
        logging.info(f"ZIP: Archivo '{zip_filename}' creado exitosamente.")
    except Exception as e:
        logging.error(f"ZIP: Error al crear el archivo ZIP '{full_zip_path}': {e}")

def leer_ultima_fecha_backup():
    if not os.path.exists(LAST_BACKUP_DATE_FILE):
        return 0.0
    try:
        with open(LAST_BACKUP_DATE_FILE, "r") as f:
            timestamp_str = f.read().strip()
            return float(timestamp_str) if timestamp_str else 0.0
    except Exception as e:
        logging.error(f"Log de fecha: Error al leer '{LAST_BACKUP_DATE_FILE}': {e}.")
        return 0.0

def escribir_ultima_fecha_backup(timestamp):
    try:
        with open(LAST_BACKUP_DATE_FILE, "w") as f:
            f.write(str(timestamp))
        logging.info(f"Log de fecha: Última fecha de backup actualizada.")
    except Exception as e:
        logging.error(f"Log de fecha: Error al escribir en '{LAST_BACKUP_DATE_FILE}': {e}")

# --- Celery Tasks ---

@app.task
def full_backup_task(origen_ruta, destino_ruta, nombre_base_zip):
    """
    Celery task to perform a full backup for a given source.
    """
    start_time = time.time()
    logging.info(f"CELERY_TASK: Iniciando backup completo para '{origen_ruta}'")

    archivos_en_origen, carpetas_vacias_en_origen = listar_contenido_recursivo(origen_ruta)

    total_errors = crear_zip_completo(origen_ruta, destino_ruta, nombre_base_zip, archivos_en_origen, carpetas_vacias_en_origen)

    duration = time.time() - start_time
    logging.info(f"CELERY_TASK: Backup completo para '{origen_ruta}' completado en {duration:.2f}s. Errores: {total_errors}")

    # This is important: a full backup should update the reference for the next incremental backup.
    escribir_ultima_fecha_backup(time.time())

    return total_errors

@app.task
def incremental_backup_task(origen_ruta, destino_ruta, nombre_base_zip, last_backup_timestamp):
    """
    Celery task to perform an incremental backup for a given source.
    """
    start_time = time.time()
    logging.info(f"CELERY_TASK: Iniciando backup incremental para '{origen_ruta}'")

    archivos_en_origen, _ = listar_contenido_recursivo(origen_ruta)

    files_to_zip = []
    archivos_error = 0

    for ruta_archivo in archivos_en_origen:
        try:
            if os.stat(ruta_archivo).st_mtime > last_backup_timestamp:
                files_to_zip.append(ruta_archivo)
        except Exception as e:
            logging.error(f"PROCESO: Error procesando archivo '{ruta_archivo}': {e}")
            archivos_error += 1

    crear_zip_incremental(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip)

    duration = time.time() - start_time
    logging.info(f"CELERY_TASK: Backup incremental para '{origen_ruta}' completado en {duration:.2f}s. Errores: {archivos_error}")

    # This is also important: an incremental backup updates the reference date.
    escribir_ultima_fecha_backup(time.time())

    return archivos_error
