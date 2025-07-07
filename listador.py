import os
import json
import logging

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
    lista_archivos = []
    lista_carpetas_vacias = []

    # Verificar si la ruta existe y es un directorio

    if not os.path.isdir(ruta_base):
        print(f"Error: La ruta '{ruta_base}' no es un directorio válido o no existe.")
        return [], []

    # os.walk() genera una tupla (dirpath, dirnames, filenames) para cada directorio
    # dirpath: La ruta del directorio actual que estamos procesando
    # dirnames: Una lista de nombres de subdirectorios en el dirpath actual
    # filenames: Una lista de nombres de archivos en el dirpath actual
    for dirpath, dirnames, filenames in os.walk(ruta_base):
        # Añadir archivos a la lista_archivos
        for filename in filenames:
            ruta_completa_archivo = os.path.join(dirpath, filename)
            lista_archivos.append(ruta_completa_archivo)

        # Comprobar si la carpeta actual está vacía (no contiene archivos ni subdirectorios)
        # Excluimos la ruta_base inicial si no está vacía, ya que generalmente no se considera
        # una "subcarpeta vacía" en el contexto de un listado.
        if not dirnames and not filenames and dirpath != ruta_base:
            lista_carpetas_vacias.append(dirpath)

    return lista_archivos, lista_carpetas_vacias

if __name__ == "__main__":

    with open("config.json", "r") as file:
        config_ubicacion = json.load(file)

    #establecer las ubicaciones
    #     arranca como indice 
    raiz = config_ubicacion[0]['origen']
    print(raiz)
    
    """
        para las ubicaciones de red no mapeadas // y luego usar las \\ 
    """
    
    # Si usaste el ejemplo de creación de estructura, usa:
    # raiz = os.path.join(os.getcwd(), "mi_directorio_test")

    import time

    def medir_tiempo(funcion, *args, **kwargs):
        inicio = time.time()
        resultado = funcion(*args, **kwargs)
        fin = time.time()
        print(f"Tiempo de ejecución: {fin - inicio:.4f} segundos")
        return resultado
    
    #archivos_encontrados, carpetas_vacias_encontradas = listar_contenido_recursivo(raiz)
    medir_tiempo(listar_contenido_recursivo,"R:\\")
    
    print("\n -- Registro de carpetas y archivos ---")

"""    if archivos_encontrados:
        for archivo in archivos_encontrados:
            print(f"- {archivo}")
"""

"""    print("\n--- Carpetas Vacías Encontradas (Ruta Completa) ---")
    if carpetas_vacias_encontradas:
        for carpeta_vacia in carpetas_vacias_encontradas:
            print(f"- {carpeta_vacia}")"""
    