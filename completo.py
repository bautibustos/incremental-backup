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
# NO usar basicConfig aquí para permitir logs dinámicos por ejecución
# El logger raíz se configurará dinámicamente en run_full_backup_process
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

# DATE_FORMAT ya se importa de incremental.py

# --- Configuración de paralelismo ---
MAX_WORKERS = 3 # Máximo de hilos concurrentes

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

    # Verificación explícita de la existencia y validez del directorio
    if not os.path.isdir(ruta_base_normalizada):
        logging.error(f"FS: Error: La ruta '{ruta_base_normalizada}' no es un directorio válido o no existe. No se puede listar el contenido.")
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
    
    # Normalizar la ruta de destino para el archivo ZIP
    full_zip_path = normalizar_ruta_larga_windows(os.path.join(destino_ruta, zip_filename))

    # Normalizar la ruta del directorio de destino antes de crearlo
    normalized_destino_ruta = normalizar_ruta_larga_windows(destino_ruta)
    os.makedirs(normalized_destino_ruta, exist_ok=True)

    logging.info(f"ZIP: Creando archivo ZIP en '{full_zip_path}' con {len(files_to_zip)} archivos y {len(empty_folders_to_add) if empty_folders_to_add else 0} carpetas vacías.")

    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    # Asegurarse de que el origen_ruta usado para relpath no tenga el prefijo \\?\
                    # ya que relpath no lo maneja bien y el arcname no debe tenerlo.
                    arcname = os.path.relpath(file_path, origen_ruta.replace('\\\\?\\', '')) 
                    # zipf.write ya maneja la ruta real del sistema de archivos, que puede tener el prefijo
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
                        # Asegurarse de que el origen_ruta usado para relpath no tenga el prefijo \\?\
                        arcname_folder = os.path.relpath(folder_path, origen_ruta.replace('\\\\?\\', '')) + '/'
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
        logging.info(f"DIAGNOSTICO: Primeros 10 carpetas vacías: {carpetas_vacias_en_origen[:10]}")
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
def run_full_backup_process(config_data, global_desired_type):
    """
    Ejecuta el proceso de backup completo para las ubicaciones de origen
    definidas en los datos de configuración, utilizando paralelismo y
    respetando la configuración de tipo_backup por origen.
    """
    # Configurar el logger para esta ejecución específica
    timestamp_run = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"full_backup_{timestamp_run}.log"
    log_filepath = os.path.join(LOGS_DIR, log_filename)
    file_handler = setup_dynamic_file_logger(log_filepath)

    try:
        logging.info("INICIO: Aplicación de Backup Completo con Zipeo (Paralelo).")
        logging.debug(f"Configuración recibida en run_full_backup_process: {json.dumps(config_data, indent=2)}")
        
        total_backup_errors = 0
        tasks_to_run = []

        if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
            config_ubicaciones = config_data['origenes']
            
            for entry in config_ubicaciones:
                # Verificar la configuración de tipo_backup por origen
                tipo_backup_override = entry.get('tipo_backup')
                should_run_full_for_origin = False

                # Solo si el tipo global deseado es COMPLETO, evaluamos el origen
                if global_desired_type == "full":
                    if tipo_backup_override is not None and isinstance(tipo_backup_override, dict):
                        if tipo_backup_override.get('completo') is True:
                            should_run_full_for_origin = True
                            logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' forzado a COMPLETO por configuración específica.")
                        elif tipo_backup_override.get('completo') is False:
                            should_run_full_for_origin = False
                            logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' excluido de COMPLETO por configuración específica.")
                        else: # tipo_backup existe pero 'completo' no es explícitamente true/false, o valor inválido
                            should_run_full_for_origin = True # Por defecto, si no es explícito, se incluye si el tipo global es full
                            logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' se ejecutará como COMPLETO (por regla global, ya que tipo_backup no es explícito).")
                    else: # No hay configuración de tipo_backup por origen, se aplica la regla global (full)
                        should_run_full_for_origin = True
                        logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' se ejecutará como COMPLETO (por regla global).")
                else: # Si el tipo global deseado NO es COMPLETO, este origen no se ejecuta como completo.
                    should_run_full_for_origin = False
                    logging.debug(f"PROCESO: Origen '{entry['origen_ruta']}' no se ejecutará como COMPLETO (tipo global: {global_desired_type}).")


                if should_run_full_for_origin:
                    # Validar que las claves necesarias existan antes de añadir a la tarea
                    if all(key in entry for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
                        tasks_to_run.append(entry)
                    else:
                        logging.error(f"ERROR: Configuración incompleta para el origen: {entry}. Faltan claves 'origen_ruta', 'destino_ruta' o 'nombre_base_zip'.")
                        total_backup_errors += 1
            
            if not tasks_to_run:
                logging.info("PROCESO: No hay tareas de backup completo para ejecutar en esta ocasión.")
                return 0

            logging.info(f"PROCESO: Se iniciarán {len(tasks_to_run)} tareas de backup completo en paralelo (máx. {MAX_WORKERS} hilos).")
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_origin = {
                    executor.submit(ejecutar_backup_completo, entry['origen_ruta'], entry['destino_ruta'], entry['nombre_base_zip']): entry
                    for entry in tasks_to_run
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
            logging.critical("ERROR: El formato del archivo config.json no es el esperado. Debe ser un objeto con la clave 'origenes' (lista).")
            total_backup_errors += 1

        logging.info(f"FIN: Aplicación de Backup Completo con Zipeo finalizada con {total_backup_errors} errores totales en el proceso de backup.")
        
        escribir_ultima_fecha_backup(time.time())

        return total_backup_errors
    finally:
        teardown_dynamic_file_logger(file_handler) # Asegura que el handler se remueva y cierre
