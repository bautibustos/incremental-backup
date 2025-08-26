import json
import logging
import datetime
from tasks import incremental_backup_task, leer_ultima_fecha_backup

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - (Incremental) - %(message)s',
    handlers=[
        logging.FileHandler("backup_log.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# --- Constantes ---
DATE_FORMAT = "%d-%m-%Y_%H-%M"

def run_incremental_backup_process(config_data, global_desired_type):
    """
    Determina qué orígenes necesitan un backup incremental y despacha las tareas a Celery.
    """
    logging.info("INICIO: Despachando tareas de Backup Incremental a Celery.")
    
    tasks_dispatched = 0
    
    # Obtenemos el timestamp del último backup una sola vez
    last_backup_timestamp_global = leer_ultima_fecha_backup()
    formatted_last_backup_date = datetime.datetime.fromtimestamp(last_backup_timestamp_global).strftime(DATE_FORMAT)
    logging.info(f"Último backup de referencia para tareas incrementales: {formatted_last_backup_date}")

    if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
        config_ubicaciones = config_data['origenes']
        
        for entry in config_ubicaciones:
            tipo_backup_override = entry.get('tipo_backup')
            should_run_incremental = False

            if tipo_backup_override is not None and isinstance(tipo_backup_override, dict):
                if tipo_backup_override.get('incremental') is True:
                    should_run_incremental = True
                    logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' forzado a INCREMENTAL.")
                elif tipo_backup_override.get('incremental') is False:
                    logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' excluido de INCREMENTAL.")
                else:
                    if global_desired_type == "incremental":
                        should_run_incremental = True
                        logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' se ejecutará como INCREMENTAL (regla global).")
            else:
                if global_desired_type == "incremental":
                    should_run_incremental = True
                    logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' se ejecutará como INCREMENTAL (regla global).")

            if should_run_incremental:
                if all(key in entry for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
                    logging.info(f"DESPACHO: Enviando tarea de backup incremental para '{entry['origen_ruta']}' a Celery.")
                    # Despachar la tarea a Celery
                    incremental_backup_task.delay(
                        entry['origen_ruta'],
                        entry['destino_ruta'],
                        entry['nombre_base_zip'],
                        last_backup_timestamp_global  # Pasamos el timestamp global
                    )
                    tasks_dispatched += 1
                else:
                    logging.error(f"ERROR: Configuración incompleta para '{entry}'. Faltan claves.")
        
        if tasks_dispatched == 0:
            logging.info("PROCESO: No hay tareas de backup incremental para despachar en esta ocasión.")
        else:
            logging.info(f"FIN: {tasks_dispatched} tareas de backup incremental han sido despachadas a Celery.")
    else:
        logging.critical("ERROR: Formato de config.json no esperado.")

    return tasks_dispatched

# Bloque de prueba
if __name__ == '__main__':
    config_file_path = "config.json"
    try:
        with open(config_file_path, "r") as file:
            config_data = json.load(file)
        logging.info(f"Configuración cargada desde '{config_file_path}' para prueba de despacho incremental.")
        # Asumimos 'incremental' para una prueba directa.
        run_incremental_backup_process(config_data, "incremental")
    except Exception as e:
        logging.critical(f"ERROR: No se pudo ejecutar la prueba de despacho incremental: {e}")