porque no guardada en el archivo last_backup_date.txt también tenga el formato DD-MM-YYYY_hh-mm. Hay una consideración importante con este cambio para el funcionamiento actual del script:

La lógica del sistema de backup incremental se basa en comparar directamente el timestamp(es una forma de etiquetar un evento con información temporal precisa) numérico de la fecha de última modificación de los archivos (os.stat().st_mtime, que es un número flotante) con el timestamp guardado del último backup.

Si guardamos la fecha en el archivo last_backup_date.txt como una cadena de texto formateada (ej. "07-07-2025_14-30"), entonces para poder compararla con os.stat().st_mtime, necesitaríamos:

    - Leer esa cadena de texto.

    - Convertirla de nuevo a un objeto datetime.

    - Convertir ese objeto datetime a un timestamp numérico.

Esto añade una capa de complejidad y hace que el sistema sea más susceptible a errores de formato si el archivo last_backup_date.txt se modifica manualmente o si el formato de la fecha cambia en el futuro. El enfoque actual (guardar el timestamp numérico y solo formatearlo para mostrarlo en el log) es más robusto porque la comparación se hace directamente entre números, lo cual es muy eficiente y menos propenso a errores de parsing.
Todo esto implica un paso mas y aumentar la complejidad del codigo.