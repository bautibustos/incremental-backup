import os
import json
import logging
import time
import zipfile
import datetime # Para formatear la fecha y hora en el nombre del zip

# Importa la función para escribir la última fecha de backup del script incremental
# Esto permite que el backup completo también actualice el punto de referencia para incrementales
from incremental import escribir_ultima_fecha_backup, LAST_BACKUP_DATE_FILE, DATE_FORMAT

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

# DATE_FORMAT ya se importa de incremental.py

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

    # Verificación explícita de la existencia y validez del directorio
    if not os.path.isdir(ruta_base):
        logging.error(f"FS: Error: La ruta '{ruta_base}' no es un directorio válido o no existe. No se puede listar el contenido.")
        return [], []

    for dirpath, dirnames, filenames in os.walk(ruta_base):
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            lista_archivos.append(ruta_completa_archivo)

        # Comprobar si la carpeta actual está vacía (no contiene archivos ni subdirectorios)
        # y no es la ruta base inicial (para no considerar la raíz como "vacía" si tiene contenido)
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

    # Crear el nombre del archivo ZIP con fecha y hora en formato DD-MM-YYYY_hh-mm
    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"COM_{nombre_base_zip}_{timestamp_str}.zip" # Prefijo COM_ para completo
    full_zip_path = os.path.join(destino_ruta, zip_filename)

    # Asegurarse de que el directorio de destino exista
    os.makedirs(destino_ruta, exist_ok=True)

    logging.info(f"ZIP: Creando archivo ZIP en '{full_zip_path}' con {len(files_to_zip)} archivos y {len(empty_folders_to_add) if empty_folders_to_add else 0} carpetas vacías.")

    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    # Calcular la ruta relativa del archivo dentro del ZIP
                    # Esto mantiene la estructura de carpetas original dentro del ZIP
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
                        # zipfile.ZipInfo para directorios debe terminar con '/'
                        arcname_folder = os.path.relpath(folder_path, origen_ruta) + '/'
                        # Crear un objeto ZipInfo para el directorio
                        zip_info = zipfile.ZipInfo(arcname_folder)
                        # Establecer atributos externos para indicar que es un directorio (opcional pero buena práctica)
                        # 0o40775 << 16 es para permisos de directorio en sistemas Unix
                        zip_info.external_attr = 0o40775 << 16 
                        zipf.writestr(zip_info, '') # Escribir una cadena vacía para el directorio
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
        zip_errors_count += len(files_to_zip) + (len(empty_folders_to_add) if empty_folders_to_add else 0) # Todos los archivos fallaron
    
    return zip_errors_count


# --- Función principal de Backup Completo ---
def ejecutar_backup_completo(origen_ruta, destino_ruta, nombre_base_zip):
    """
    Ejecuta el proceso de backup completo para una ruta de origen dada,
    zipeando todos los archivos y carpetas vacías encontrados.
    """
    start_time = time.time() # Registrar el tiempo de inicio
    logging.info(f"PROCESO: Iniciando backup completo para el origen: '{origen_ruta}'")
    
    archivos_en_origen, carpetas_vacias_en_origen = listar_contenido_recursivo(origen_ruta)
    
    # --- DIAGNOSTICO ---
    logging.info(f"DIAGNOSTICO: Archivos encontrados por el listado: {len(archivos_en_origen)}")
    if len(archivos_en_origen) > 0 and len(archivos_en_origen) < 10: # Solo imprimir lista completa si es pequeña
        logging.info(f"DIAGNOSTICO: Lista de archivos (todos): {archivos_en_origen}")
    elif len(archivos_en_origen) >= 10:
        logging.info(f"DIAGNOSTICO: Primeros 10 archivos: {archivos_en_origen[:10]}")
    else:
        logging.info(f"DIAGNOSTICO: No se encontraron archivos en '{origen_ruta}'.")

    logging.info(f"DIAGNOSTICO: Carpetas vacías encontradas: {len(carpetas_vacias_en_origen)}")
    if len(carpetas_vacias_en_origen) > 0 and len(carpetas_vacias_en_origen) < 10: # Solo imprimir lista completa si es pequeña
        logging.info(f"DIAGNOSTICO: Lista de carpetas vacías (todas): {carpetas_vacias_en_origen}")
    elif len(carpetas_vacias_en_origen) >= 10:
        logging.info(f"DIAGNOSTICO: Primeras 10 carpetas vacías: {carpetas_vacias_en_origen[:10]}")
    else:
        logging.info(f"DIAGNOSTICO: No se encontraron carpetas vacías en '{origen_ruta}'.")
    # --- FIN DIAGNOSTICO ---

    files_to_zip = [] # Lista para almacenar todos los archivos
    archivos_procesados = 0
    archivos_error_listado = 0 # Errores durante el listado o acceso inicial

    for ruta_archivo in archivos_en_origen:
        try:
            # Para un backup completo, todos los archivos son considerados para el zipeo
            files_to_zip.append(ruta_archivo)
            archivos_procesados += 1
            logging.debug(f"PROCESO: Añadiendo '{ruta_archivo}' a la lista de zipeo.")

        except FileNotFoundError:
            logging.warning(f"PROCESO: Archivo '{ruta_archivo}' no encontrado durante el procesamiento (posiblemente eliminado).")
            archivos_error_listado += 1 # Considerar como error para seguimiento
        except PermissionError:
            logging.error(f"PROCESO: Permiso denegado para acceder a '{ruta_archivo}'.")
            archivos_error_listado += 1
        except Exception as e:
            logging.error(f"PROCESO: Error inesperado al procesar '{ruta_archivo}': {e}")
            archivos_error_listado += 1
            
    # Después de procesar todos los archivos, crear el ZIP
    # Se pasa también la lista de carpetas vacías
    zip_creation_errors = crear_zip_completo(origen_ruta, destino_ruta, nombre_base_zip, files_to_zip, carpetas_vacias_en_origen)
    
    total_errors = archivos_error_listado + zip_creation_errors

    end_time = time.time() # Registrar el tiempo de finalización
    duration = end_time - start_time # Calcular la duración
    logging.info(f"PROCESO: Backup completo para '{origen_ruta}' completado en {duration:.2f} segundos.")
    logging.info(f"PROCESO: Resumen - Archivos procesados (para zipear): {archivos_procesados}, Errores totales (listado + zipeo): {total_errors}")
    return total_errors # Return total errors for the scheduler


import concurrent.futures
# --- Función para ejecutar el proceso de backup completo ---
def run_full_backup_process(config_data):
    """
    Ejecuta el proceso de backup completo para todas las ubicaciones de origen
    definidas en los datos de configuración.
    """
    logging.info("INICIO: Aplicación de Backup Completo con Zipeo.")
    logging.debug(f"Configuración recibida en run_full_backup_process: {json.dumps(config_data, indent=2)}") # Diagnóstico
    
    total_backup_errors = 0

    programacion_config = config_data.get("programacion", {})
    max_threads = programacion_config.get("max_threads", 1)  # Default a 1 hilo si no está definido
    logging.info(f"THREADING: Usando un máximo de {max_threads} hilos para el backup completo.")


    # 1. Iterar sobre cada ubicación de origen en el archivo de configuración
    # Se espera que config_data tenga la clave 'origenes'
    if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
        config_ubicaciones = config_data['origenes']
        # Se esperan las claves 'origen_ruta', 'destino_ruta', 'nombre_base_zip'
        if all(key in d for d in config_ubicaciones for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                # Creamos un futuro para cada tarea de backup
                future_to_origen = {
                    executor.submit(
                        ejecutar_backup_completo,
                        config_entry['origen_ruta'],
                        config_entry['destino_ruta'],
                        config_entry['nombre_base_zip']
                    ): config_entry['origen_ruta']
                    for config_entry in config_ubicaciones
                }

                for future in concurrent.futures.as_completed(future_to_origen):
                    origen_ruta = future_to_origen[future]
                    try:
                        errors = future.result()
                        total_backup_errors += errors
                        logging.info(f"THREADING: Backup para '{origen_ruta}' completado con {errors} errores.")
                    except Exception as exc:
                        logging.critical(f"THREADING: Backup para '{origen_ruta}' generó una excepción: {exc}")
                        total_backup_errors += 1
        else:
            logging.critical("ERROR: El formato de la clave 'origenes' en config.json no es el esperado. Debe ser una lista de objetos con las claves 'origen_ruta', 'destino_ruta' y 'nombre_base_zip'.")
            total_backup_errors += 1 # Indicar un error de configuración
    else:
        logging.critical("ERROR: El formato del archivo config.json no es el esperado. Debe ser un objeto con la clave 'origenes' (lista).")
        total_backup_errors += 1 # Indicar un error de configuración

    logging.info(f"FIN: Aplicación de Backup Completo con Zipeo finalizada con {total_backup_errors} errores totales en el proceso de backup.")
    
    # --- Modificación para registrar la fecha del backup completo ---
    # Escribir la fecha actual como la última fecha de backup para el incremental
    escribir_ultima_fecha_backup(time.time())
    # --- Fin de la modificación ---

    return total_backup_errors # Return total errors from the backup process

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
                "nombre_base_zip": "backup_origen_1_completo"
            },
            {
                "origen_ruta": os.path.join(os.getcwd(), "test_origen_2"),
                "destino_ruta": os.path.join(os.getcwd(), "backups", "origen_2"),
                "nombre_base_zip": "backup_origen_2_completo"
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

    run_full_backup_process(config_data)
