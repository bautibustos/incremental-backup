@echo off
REM Script para ejecutar el planificador de backups en Python

REM Cambia al directorio donde se encuentra este script
cd /d "%~dp0"

REM Ruta al ejecutable de Python
REM Asegúrate de que esta ruta sea correcta para tu instalación de Python
SET PYTHON_EXE=D:\incremental-backup\recursos\Scripts\python.exe

REM Nombre del script principal a ejecutar
SET MAIN_SCRIPT=main.py

REM Ejecuta el script de Python
"%PYTHON_EXE%" "%MAIN_SCRIPT%"

REM Pausa la ventana de comandos al finalizar (opcional, útil para ver mensajes si se ejecuta directamente)
REM PAUSE