import json
import logging
from tasks import full_backup_task

# --- Configuración de Logging ---
# El logging principal ahora está en `main.py` y `tasks.py`,
# pero mantenemos un logger aquí para mensajes específicos de este módulo.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - (Completo) - %(message)s',
    handlers=[
        logging.FileHandler("backup_log.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def run_full_backup_process(config_data, global_desired_type):
    """
    Determina qué orígenes necesitan un backup completo y despacha las tareas a Celery.
    """
    logging.info("INICIO: Despachando tareas de Backup Completo a Celery.")
    
    tasks_dispatched = 0
    
    if isinstance(config_data, dict) and 'origenes' in config_data and isinstance(config_data['origenes'], list):
        config_ubicaciones = config_data['origenes']
        
        for entry in config_ubicaciones:
            tipo_backup_override = entry.get('tipo_backup')
            should_run_full = False

            if tipo_backup_override is not None and isinstance(tipo_backup_override, dict):
                if tipo_backup_override.get('completo') is True:
                    should_run_full = True
                    logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' forzado a COMPLETO.")
                elif tipo_backup_override.get('completo') is False:
                    logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' excluido de COMPLETO.")
                else:
                    if global_desired_type == "full":
                        should_run_full = True
                        logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' se ejecutará como COMPLETO (regla global).")
            else:
                if global_desired_type == "full":
                    should_run_full = True
                    logging.info(f"PROCESO: Origen '{entry['origen_ruta']}' se ejecutará como COMPLETO (regla global).")

            if should_run_full:
                if all(key in entry for key in ['origen_ruta', 'destino_ruta', 'nombre_base_zip']):
                    logging.info(f"DESPACHO: Enviando tarea de backup completo para '{entry['origen_ruta']}' a la cola de Celery.")
                    # Despachar la tarea a Celery en lugar de ejecutarla directamente
                    full_backup_task.delay(
                        entry['origen_ruta'],
                        entry['destino_ruta'],
                        entry['nombre_base_zip']
                    )
                    tasks_dispatched += 1
                else:
                    logging.error(f"ERROR: Configuración incompleta para '{entry}'. Faltan claves.")
        
        if tasks_dispatched == 0:
            logging.info("PROCESO: No hay tareas de backup completo para despachar en esta ocasión.")
        else:
            logging.info(f"FIN: {tasks_dispatched} tareas de backup completo han sido despachadas a Celery.")
    else:
        logging.critical("ERROR: Formato de config.json no esperado.")

    # El retorno de errores ya no es síncrono. Celery manejará los errores de las tareas.
    return tasks_dispatched

# El bloque `if __name__ == '__main__':` se puede mantener para pruebas,
# pero ahora solo despachará tareas, no las ejecutará.
if __name__ == '__main__':
    config_file_path = "config.json"
    try:
        with open(config_file_path, "r") as file:
            config_data = json.load(file)
        logging.info(f"Configuración cargada desde '{config_file_path}' para prueba de despacho.")
        # Asumimos 'full' para una prueba directa.
        run_full_backup_process(config_data, "full")
    except Exception as e:
        logging.critical(f"ERROR: No se pudo ejecutar la prueba de despacho: {e}")