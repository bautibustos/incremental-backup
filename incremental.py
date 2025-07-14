import os
import json
import logging
import time
import zipfile
import datetime # Para formatear la fecha y hora en el nombre del zip

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

# --- Archivo para la última fecha de backup ---
LAST_BACKUP_DATE_FILE = "last_backup_date.txt"
DATE_FORMAT = "%d-%m-%Y_%H-%M" # Formato de fecha y hora deseado para logs y nombres de zip

def leer_ultima_fecha_backup():
    """
    Lee el timestamp numérico de la última fecha de backup desde un archivo.
    Retorna 0.0 si el archivo no existe o está vacío.
    """
    if not os.path.exists(LAST_BACKUP_DATE_FILE):
        logging.info(f"Log de fecha: Archivo '{LAST_BACKUP_DATE_FILE}' no encontrado. Asumiendo backup inicial.")
        return 0.0 # Retorna 0.0 para considerar todos los archivos como nuevos
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
        # Para el log, formateamos el timestamp para que sea legible
        formatted_date_str = datetime.datetime.fromtimestamp(timestamp).strftime(DATE_FORMAT)
        logging.info(f"Log de fecha: Última fecha de backup ({formatted_date_str}) escrita en '{LAST_BACKUP_DATE_FILE}'.")
    except Exception as e:
        logging.error(f"Log de fecha: Error al escribir en '{LAST_BACKUP_DATE_FILE}': {e}")


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

    # Crear el nombre del archivo ZIP con fecha y hora en formato DD-MM-YYYY_hh-mm
    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"INC_{nombre_base_zip}_{timestamp_str}.zip" # Prefijo INC_ para incremental
    full_zip_path = os.path.join(destino_ruta, zip_filename)

    # Asegurarse de que el directorio de destino exista
    os.makedirs(destino_ruta, exist_ok=True)

    logging.info(f"ZIP: Creando archivo ZIP en '{full_zip_path}' con {len(files_to_zip)} archivos.")

    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                # Calcular la ruta relativa del archivo dentro del ZIP
                # Esto mantiene la estructura de carpetas original dentro del ZIP
                arcname = os.path.relpath(file_path, origen_ruta)
                zipf.write(file_path, arcname)
                logging.debug(f"ZIP: Añadido '{file_path}' como '{arcname}' al ZIP.")
        logging.info(f"ZIP: Archivo ZIP '{zip_filename}' creado exitosamente.")
    except Exception as e:
        logging.error(f"ZIP: Error al crear el archivo ZIP '{full_zip_path}': {e}")


# --- Función principal de Backup Incremental (simplificada) ---
def ejecutar_backup_incremental(origen_ruta, destino_ruta, nombre_base_zip, last_backup_timestamp):
    """
    Ejecuta el proceso de backup incremental para una ruta de origen dada,
    basado en la fecha de la última ejecución, y zipea los archivos modificados.
    """
    start_time = time.time() # Registrar el tiempo de inicio
    logging.info(f"PROCESO: Iniciando backup incremental para el origen: '{origen_ruta}'")
    
    archivos_en_origen, _ = listar_contenido_recursivo(origen_ruta)
    
    files_to_zip = [] # Lista para almacenar los archivos modificados/nuevos
    archivos_modificados_o_nuevos = 0
    archivos_sin_cambios = 0
    archivos_error = 0

    for ruta_archivo in archivos_en_origen:
        try:
            # Obtener metadatos actuales del archivo
            stats = os.stat(ruta_archivo)
            fecha_mod_actual = stats.st_mtime

            if fecha_mod_actual > last_backup_timestamp:
                # El archivo ha sido modificado o creado desde el último backup
                logging.info(f"PROCESO: '{ruta_archivo}' modificado/nuevo desde el último backup. Añadiendo a la lista de zipeo.")
                files_to_zip.append(ruta_archivo) # Añadir a la lista para zipear
                archivos_modificados_o_nuevos += 1
            else:
                logging.debug(f"PROCESO: '{ruta_archivo}' sin cambios desde el último backup.")
                archivos_sin_cambios += 1

        except FileNotFoundError:
            logging.warning(f"PROCESO: Archivo '{ruta_archivo}' no encontrado durante el procesamiento (posiblemente eliminado).")
            archivos_error += 1 # Considerar como error para seguimiento
        except PermissionError:
            logging.error(f"PROCESO: Permiso denegado para acceder a '{ruta_archivo}'.")
            archivos_error += 1
        except Exception as e:
            logging.error(f"PROCESO: Error inesperado al procesar '{ruta_archivo}': {e}")
            archivos_error += 1
            
    # Después de procesar todos los archivos, crear el ZIP
    crear_zip_incremental(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip)

    end_time = time.time() # Registrar el tiempo de finalización
    duration = end_time - start_time # Calcular la duración
    logging.info(f"PROCESO: Backup incremental para '{origen_ruta}' completado en {duration:.2f} segundos.")
    logging.info(f"PROCESO: Resumen - Nuevos/Modificados: {archivos_modificados_o_nuevos}, Sin cambios: {archivos_sin_cambios}, Errores: {archivos_error}")
    return archivos_error # Retorna el número de errores para el planificador


# --- Función para ejecutar el proceso de backup incremental ---
def run_incremental_backup_process(config_data):
    """
    Ejecuta el proceso de backup incremental para todas las ubicaciones de origen
    definidas en los datos de configuración.
    """
    logging.info("INICIO: Aplicación de Backup Incremental (Simplificada con Zipeo).")
    logging.debug(f"Configuración recibida en run_incremental_backup_process: {json.dumps(config_data, indent=2)}") # Diagnóstico

    total_backup_errors = 0

    # 1. Leer la última fecha de backup antes de iniciar el procesamiento de orígenes
    last_backup_timestamp_global = leer_ultima_fecha_backup()
    # Formatear la fecha para el log
    formatted_last_backup_date = datetime.datetime.fromtimestamp(last_backup_timestamp_global).strftime(DATE_FORMAT)
    logging.info(f"Último backup registrado: {formatted_last_backup_date}")

    # 2. Iterar sobre cada ubicación de origen en el archivo de configuración
    # Se espera que config_data tenga la clave 'origenes'
    if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
        config_ubicaciones = config_data['origenes']
        # Se esperan las claves 'origen_ruta', 'destino_ruta', 'nombre_base_zip'
        if all(key in d for d in config_ubicaciones for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
            for i, config_entry in enumerate(config_ubicaciones):
                origen_ruta = config_entry['origen_ruta']
                destino_ruta = config_entry['destino_ruta']
                nombre_base_zip = config_entry['nombre_base_zip']

                logging.info(f"\n--- Procesando origen [{i+1}/{len(config_ubicaciones)}]: '{origen_ruta}' ---")
                errors = ejecutar_backup_incremental(origen_ruta, destino_ruta, nombre_base_zip, last_backup_timestamp_global)
                total_backup_errors += errors
        else:
            logging.critical("ERROR: El formato de la clave 'origenes' en config.json no es el esperado. Debe ser una lista de objetos con las claves 'origen_ruta', 'destino_ruta' y 'nombre_base_zip'.")
            total_backup_errors += 1 # Indicar un error de configuración
    else:
        logging.critical("ERROR: El formato del archivo config.json no es el esperado. Debe ser un objeto con la clave 'origenes' (lista).")
        total_backup_errors += 1 # Indicar un error de configuración

    # 3. Escribir la fecha actual como la última fecha de backup
    escribir_ultima_fecha_backup(time.time())

    logging.info(f"FIN: Aplicación de Backup Incremental (Simplificada con Zipeo) finalizada con {total_backup_errors} errores totales en el proceso de backup.")
    return total_backup_errors # Retorna el número total de errores del proceso de backup

# El bloque principal de ejecución (if __name__ == '__main__':) se mantiene para pruebas directas
if __name__ == '__main__':
    config_file_path = "config.json"
    
    # Define example_config para asegurar un estado ejecutable si config.json falta o es inválido
    example_config = {
        "programacion": {
            "hora_backup": "02:00",
            "intervalo_verificacion_segundos": 60
        },
        "origenes": [
            {
                "origen_ruta": os.path.join(os.getcwd(), "test_origen_1"),
                "destino_ruta": os.path.join(os.getcwd(), "backups", "origen_1"),
                "nombre_base_zip": "backup_origen_1_incremental"
            },
            {
                "origen_ruta": os.path.join(os.getcwd(), "test_origen_2"),
                "destino_ruta": os.path.join(os.getcwd(), "backups", "origen_2"),
                "nombre_base_zip": "backup_origen_2_incremental"
            }
        ]
    }

    # Crear directorios de prueba para el ejemplo si no existen
    for entry in example_config['origenes']:
        os.makedirs(entry["destino_ruta"], exist_ok=True)
        path = entry['origen_ruta']
        if not os.path.exists(path):
            os.makedirs(path)
            logging.info(f"Directorio de prueba '{path}' creado.")
            with open(os.path.join(path, "example_file_root.txt"), "w") as f:
                f.write("This is a root example file.")
            subfolder_path = os.path.join(path, "subfolder_example")
            os.makedirs(subfolder_path, exist_ok=True)
            with open(os.path.join(subfolder_path, "sub_file.log"), "w") as f:
                f.write("Log content in subfolder.")
            empty_subfolder_path = os.path.join(path, "empty_subfolder")
            os.makedirs(empty_subfolder_path, exist_ok=True)
        else:
            logging.info(f"Directorio de prueba '{path}' ya existe.")


    # Intentar cargar config.json, si falla, usar example_config
    try:
        with open(config_file_path, "r") as file:
            config_data = json.load(file)
        logging.info(f"Configuración cargada desde '{config_file_path}'.")
    except FileNotFoundError:
        logging.warning(f"Archivo de configuración '{config_file_path}' no encontrado. Usando configuración de ejemplo.")
        config_data = example_config
    except json.JSONDecodeError as e:
        logging.critical(f"ERROR: Error al parsear '{config_file_path}'. Verifique el formato JSON: {e}. Usando configuración de ejemplo.")
        config_data = example_config
    except Exception as e:
        logging.critical(f"ERROR: Error inesperado al cargar la configuración: {e}. Usando configuración de ejemplo.")
        config_data = example_config

    run_incremental_backup_process(config_data)
