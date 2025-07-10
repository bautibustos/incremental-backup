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

# --- Formato de fecha y hora deseado para logs y nombres de zip ---
DATE_FORMAT = "%d-%m-%Y_%H-%M" 

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

def crear_zip_completo(origen_path, destino_path, nombre_zip_base, files_to_zip, empty_folders_to_add=None):
    """
    Crea un archivo ZIP con todos los archivos y opcionalmente carpetas vacías,
    manteniendo su estructura de directorios relativa al origen.
    El nombre del ZIP incluye la fecha y hora.

    Args:
        origen_path (str): La ruta base de donde provienen los archivos.
        destino_path (str): La ruta donde se guardará el archivo ZIP.
        nombre_zip_base (str): El nombre base del archivo ZIP (sin fecha/hora ni extensión).
        files_to_zip (list): Una lista de rutas completas de los archivos a incluir en el ZIP.
        empty_folders_to_add (list, optional): Lista de rutas completas de carpetas vacías a añadir.
                                               Por defecto es None.

    Returns:
        int: El número de errores encontrados al añadir archivos/carpetas al ZIP.
    """
    zip_errors_count = 0

    if not files_to_zip and not (empty_folders_to_add and len(empty_folders_to_add) > 0):
        logging.info(f"ZIP: No hay archivos ni carpetas vacías para zipear en el origen '{origen_path}'.")
        return zip_errors_count

    # Crear el nombre del archivo ZIP con fecha y hora en formato DD-MM-YYYY_hh-mm
    timestamp_str = datetime.datetime.now().strftime(DATE_FORMAT)
    zip_filename = f"COM_{nombre_zip_base}_{timestamp_str}.zip"
    full_zip_path = os.path.join(destino_path, zip_filename)

    # Asegurarse de que el directorio de destino exista
    os.makedirs(destino_path, exist_ok=True)

    logging.info(f"ZIP: Creando archivo ZIP en '{full_zip_path}' con {len(files_to_zip)} archivos y {len(empty_folders_to_add) if empty_folders_to_add else 0} carpetas vacías.")

    try:
        with zipfile.ZipFile(full_zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in files_to_zip:
                try:
                    # Calcular la ruta relativa del archivo dentro del ZIP
                    # Esto mantiene la estructura de carpetas original dentro del ZIP
                    arcname = os.path.relpath(file_path, origen_path)
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
                        arcname_folder = os.path.relpath(folder_path, origen_path) + '/'
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
def ejecutar_backup_completo(origen_path, destino_path, nombre_zip_base):
    """
    Ejecuta el proceso de backup completo para una ruta de origen dada,
    zipeando todos los archivos y carpetas vacías encontrados.
    """
    start_time = time.time() # Registrar el tiempo de inicio
    logging.info(f"PROCESO: Iniciando backup completo para el origen: '{origen_path}'")
    
    archivos_en_origen, carpetas_vacias_en_origen = listar_contenido_recursivo(origen_path)
    
    # --- DIAGNOSTICO ---
    logging.info(f"DIAGNOSTICO: Archivos encontrados por el listado: {len(archivos_en_origen)}")
    if len(archivos_en_origen) > 0 and len(archivos_en_origen) < 10: # Solo imprimir lista completa si es pequeña
        logging.info(f"DIAGNOSTICO: Lista de archivos (todos): {archivos_en_origen}")
    elif len(archivos_en_origen) >= 10:
        logging.info(f"DIAGNOSTICO: Primeros 10 archivos: {archivos_en_origen[:10]}")
    else:
        logging.info(f"DIAGNOSTICO: No se encontraron archivos en '{origen_path}'.")

    logging.info(f"DIAGNOSTICO: Carpetas vacías encontradas: {len(carpetas_vacias_en_origen)}")
    if len(carpetas_vacias_en_origen) > 0 and len(carpetas_vacias_en_origen) < 10: # Solo imprimir lista completa si es pequeña
        logging.info(f"DIAGNOSTICO: Lista de carpetas vacías (todas): {carpetas_vacias_en_origen}")
    elif len(carpetas_vacias_en_origen) >= 10:
        logging.info(f"DIAGNOSTICO: Primeras 10 carpetas vacías: {carpetas_vacias_en_origen[:10]}")
    else:
        logging.info(f"DIAGNOSTICO: No se encontraron carpetas vacías en '{origen_path}'.")
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
    zip_creation_errors = crear_zip_completo(origen_path, destino_path, nombre_zip_base, files_to_zip, carpetas_vacias_en_origen)
    
    total_errors = archivos_error_listado + zip_creation_errors

    end_time = time.time() # Registrar el tiempo de finalización
    duration = end_time - start_time # Calcular la duración
    logging.info(f"PROCESO: Backup completo para '{origen_path}' completado en {duration:.2f} segundos.")
    logging.info(f"PROCESO: Resumen - Archivos procesados (para zipear): {archivos_procesados}, Errores totales (listado + zipeo): {total_errors}")


# --- Bloque Principal de Ejecución ---
if __name__ == '__main__':
    logging.info("INICIO: Aplicación de Backup Completo con Zipeo.")

    # 1. Cargar configuraciones desde config.json
    config_file_path = "config.json"
    # Definir example_config directamente, ya no se crea el archivo si no existe
    example_config = [
        {
            "origen": os.path.join(os.getcwd(), "test_origin_1"),
            "destino": os.path.join(os.getcwd(), "backups", "origin_1"),
            "nombre_zip_base": "full_backup_origen_1"
        },
        {
            "origen": os.path.join(os.getcwd(), "test_origin_2"),
            "destino": os.path.join(os.getcwd(), "backups", "origin_2"),
            "nombre_zip_base": "full_backup_origen_2"
        }
    ]
    # Asegurarse de que los directorios de destino existan para el ejemplo
    for entry in example_config:
        os.makedirs(entry["destino"], exist_ok=True)

    # Ahora se asume que config.json existe y es válido, o se usa example_config
    try:
        with open(config_file_path, "r") as file:
            config_ubicaciones = json.load(file)
        logging.info(f"Configuración cargada desde '{config_file_path}'.")
    except FileNotFoundError:
        logging.warning(f"Archivo de configuración '{config_file_path}' no encontrado. Usando configuración de ejemplo.")
        config_ubicaciones = example_config # Usar la configuración de ejemplo si el archivo no existe
    except json.JSONDecodeError as e:
        logging.critical(f"ERROR: Error al parsear '{config_file_path}'. Verifique el formato JSON: {e}. Usando configuración de ejemplo.")
        config_ubicaciones = example_config # Usar la configuración de ejemplo si el JSON es inválido
    except Exception as e:
        logging.critical(f"ERROR: Error inesperado al cargar la configuración: {e}. Usando configuración de ejemplo.")
        config_ubicaciones = example_config # Usar la configuración de ejemplo para otros errores


    # 2. Iterar sobre cada ubicación de origen en el archivo de configuración
    for i, config_entry in enumerate(config_ubicaciones):
        origen_path = config_entry['origen']
        destino_path = config_entry['destino']
        nombre_zip_base = config_entry['nombre_zip_base']

        logging.info(f"\n--- Procesando origen [{i+1}/{len(config_ubicaciones)}]: '{origen_path}' ---")
        # Llamada a la función de backup completo
        ejecutar_backup_completo(origen_path, destino_path, nombre_zip_base)

    logging.info("FIN: Aplicación de Backup Completo con Zipeo finalizada.")
