import time
import datetime
import json
import os
import logging

# Importa las funciones principales de los scripts de backup
from completo import run_full_backup_process
from incremental import run_incremental_backup_process

# --- Configuración de Logging para el Scheduler ---
# Configura el logger para guardar mensajes en un archivo y mostrarlos en la consola
logging.basicConfig(
    level=logging.INFO, # Nivel mínimo de mensajes a registrar
    format='%(asctime)s - %(levelname)s - (Scheduler) - %(message)s',
    handlers=[
        logging.FileHandler("scheduler_log.log", encoding='utf-8'), # Guarda los logs del scheduler
        logging.StreamHandler() # Muestra los logs en la consola
    ]
)

# Archivo para registrar la última fecha en que se ejecutó el backup
# Esto es para asegurar que el backup solo se ejecute una vez al día a la hora programada.
LAST_SCHEDULED_RUN_DATE_FILE = "last_backup_scheduled_date.txt"

def read_last_scheduled_run_date():
    """Lee la última fecha registrada en que el backup fue programado con éxito."""
    if os.path.exists(LAST_SCHEDULED_RUN_DATE_FILE):
        try:
            with open(LAST_SCHEDULED_RUN_DATE_FILE, "r") as f:
                date_str = f.read().strip()
                if date_str:
                    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception as e:
            logging.error(f"Scheduler: Error al leer la última fecha de ejecución programada de '{LAST_SCHEDULED_RUN_DATE_FILE}': {e}")
    return None

def write_last_scheduled_run_date(date_obj):
    """Escribe la fecha actual para registrar cuándo se programó el backup con éxito."""
    try:
        with open(LAST_SCHEDULED_RUN_DATE_FILE, "w") as f:
            f.write(date_obj.strftime("%Y-%m-%d"))
        logging.info(f"Scheduler: Última fecha de ejecución programada registrada: {date_obj.strftime('%Y-%m-%d')}")
    except Exception as e:
        logging.error(f"Scheduler: Error al escribir la última fecha de ejecución programada en '{LAST_SCHEDULED_RUN_DATE_FILE}': {e}")


def main_scheduler_loop(config_file_path="config.json"):
    """
    Bucle principal del planificador que verifica la hora y ejecuta el backup.
    """
    logging.info("Scheduler: Iniciando el planificador de backups.")

    # Cargar configuración (incluyendo el horario y tipo de backup)
    try:
        with open(config_file_path, "r") as f:
            config_data = json.load(f)
        
        schedule_config = config_data.get("programacion")
        if not schedule_config:
            logging.critical("Scheduler: La sección 'programacion' no se encontró en config.json. Saliendo.")
            return
        
        hora_backup_str = schedule_config.get("hora_backup")
        intervalo_verificacion_segundos = schedule_config.get("intervalo_verificacion_segundos", 60) # Default a 60 segundos
        modo_prueba = schedule_config.get("modo_prueba", False) # Nueva clave para el modo de prueba
        
        if not hora_backup_str and not modo_prueba: # Si no es modo prueba, la hora es obligatoria
            logging.critical("Scheduler: La clave 'hora_backup' no se encontró en la sección 'programacion' de config.json y el modo de prueba está deshabilitado. Saliendo.")
            return

        # Parsear la hora de backup deseada (solo si no estamos en modo prueba)
        backup_hour = None
        backup_minute = None
        if not modo_prueba:
            try:
                backup_hour, backup_minute = map(int, hora_backup_str.split(':'))
            except ValueError:
                logging.critical(f"Scheduler: Formato de 'hora_backup' inválido en config.json: '{hora_backup_str}'. Debe ser HH:MM. Saliendo.")
                return

        logging.info(f"Scheduler: Modo de prueba: {'Activado' if modo_prueba else 'Desactivado'}")
        if not modo_prueba:
            logging.info(f"Scheduler: Backup programado para las {hora_backup_str} cada día, con tipo determinado por el día de la semana (y por origen).")
            logging.info(f"Scheduler: Intervalo de verificación: {intervalo_verificacion_segundos} segundos.")

    except FileNotFoundError:
        logging.critical(f"Scheduler: Archivo de configuración '{config_file_path}' no encontrado. Asegúrese de que exista. Saliendo.")
        return
    except json.JSONDecodeError as e:
        logging.critical(f"Scheduler: Error al parsear '{config_file_path}'. Verifique el formato JSON: {e}. Saliendo.")
        return
    except Exception as e:
        logging.critical(f"Scheduler: Error inesperado al cargar la configuración: {e}. Saliendo.")
        return

    # --- Lógica del modo de prueba ---
    if modo_prueba:
        logging.info("Scheduler: Ejecutando en MODO DE PRUEBA: Se realizará un backup incremental seguido de uno completo (según configuración por origen).")
        
        # En modo de prueba, llamamos a ambos tipos de backup, y cada uno decidirá qué orígenes procesar.
        logging.info("Scheduler: Iniciando ejecución de backups incrementales en modo de prueba...")
        run_incremental_backup_process(config_data, "incremental") # Pasamos "incremental" como tipo global para que se consideren los overrides
        logging.info("Scheduler: Backups incrementales en modo de prueba completados.")
        
        logging.info("Scheduler: Iniciando ejecución de backups completos en modo de prueba...")
        run_full_backup_process(config_data, "full") # Pasamos "full" como tipo global para que se consideren los overrides
        logging.info("Scheduler: Backups completos en modo de prueba completados.")
        
        logging.info("Scheduler: MODO DE PRUEBA finalizado. El script terminará.")
        return # Termina el script después de la ejecución de prueba

    # --- Lógica del temporizador (solo si modo_prueba es False) ---
    last_scheduled_run_date = read_last_scheduled_run_date()
    logging.info(f"Scheduler: Última fecha de ejecución programada registrada: {last_scheduled_run_date if last_scheduled_run_date else 'Ninguna'}")

    while True:
        now = datetime.datetime.now()
        current_date = now.date()
        current_hour = now.hour
        current_minute = now.minute
        current_weekday = now.weekday() # Lunes es 0, Domingo es 6

        # Determinar el tipo de backup global según el día de la semana
        if 0 <= current_weekday <= 4: # Lunes (0) a Viernes (4)
            global_desired_type = "incremental"
        else: # Sábado (5) o Domingo (6)
            global_desired_type = "full"

        # Verificar si es la hora programada y si no se ha ejecutado hoy
        if current_hour == backup_hour and current_minute >= backup_minute and \
           (last_scheduled_run_date is None or current_date > last_scheduled_run_date):
            
            logging.info(f"Scheduler: ¡Es hora de ejecutar el backup! ({now.strftime('%H:%M')}) - Día de la semana: {current_weekday} (Tipo global: {global_desired_type.upper()})")
            
            # Ejecutar el proceso de backup. Cada función de backup (completo/incremental)
            # filtrará los orígenes según su tipo_backup específico o el tipo global.
            if global_desired_type == "full":
                logging.info("Scheduler: Ejecutando procesos de backup (tipo global: COMPLETO).")
                run_full_backup_process(config_data, global_desired_type) 
                # Aunque el tipo global sea completo, también llamamos al incremental por si hay orígenes forzados a incremental.
                run_incremental_backup_process(config_data, global_desired_type)
            elif global_desired_type == "incremental":
                logging.info("Scheduler: Ejecutando procesos de backup (tipo global: INCREMENTAL).")
                run_incremental_backup_process(config_data, global_desired_type)
                # Aunque el tipo global sea incremental, también llamamos al completo por si hay orígenes forzados a completo.
                run_full_backup_process(config_data, global_desired_type)
            
            # Registrar la fecha de ejecución programada
            write_last_scheduled_run_date(current_date)
            last_scheduled_run_date = current_date # Actualizar la variable en memoria
            
            logging.info("Scheduler: Backup completado. Esperando hasta el próximo día programado.")
        else:
            logging.debug(f"Scheduler: Esperando la hora programada. Actual: {now.strftime('%H:%M')}, Programado: {hora_backup_str}. Última ejecución programada: {last_scheduled_run_date}")

        time.sleep(intervalo_verificacion_segundos) # Esperar antes de la próxima verificación

if __name__ == '__main__':
    main_scheduler_loop()