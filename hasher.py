import hashlib
import os

def generar_hash_archivo(ruta_archivo, algoritmo='sha256', chunk_size=4096):
    """
    Genera un hash del contenido de un archivo usando el algoritmo especificado.
    Lee el archivo en bloques para manejar archivos grandes eficientemente.

    Args:
        ruta_archivo (str): La ruta del archivo al que se le calculará el hash.
        algoritmo (str): El algoritmo de hash a usar (ej. 'md5', 'sha1', 'sha256', 'sha512').
        chunk_size (int): Tamaño de los bloques a leer del archivo (en bytes).

    Returns:
        str: El hash hexadecimal del contenido del archivo, o None si hay un error.
    """
    if not os.path.exists(ruta_archivo):
        print(f"Error: El archivo '{ruta_archivo}' no existe.")
        return None
    if not os.path.isfile(ruta_archivo):
        print(f"Error: '{ruta_archivo}' no es un archivo válido.")
        return None

    # Create a hash object based on the chosen algorithm
    if algoritmo == 'md5':
        hasher = hashlib.md5()
    elif algoritmo == 'sha1':
        hasher = hashlib.sha1()
    elif algoritmo == 'sha256':
        hasher = hashlib.sha256()
    elif algoritmo == 'sha512':
        hasher = hashlib.sha512()
    else:
        print(f"Error: Algoritmo '{algoritmo}' no soportado o no reconocido.")
        return None

    try:
        with open(ruta_archivo, 'rb') as f: # Open in binary read mode
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break # End of file
                hasher.update(chunk)
        return hasher.hexdigest()
    except IOError as e:
        print(f"Error de lectura del archivo '{ruta_archivo}': {e}")
        return None

# --- Demostración simplificada con archivo ---
if __name__ == "__main__":
    print("--- Demostración Simplificada de hashlib con archivos ---")

    # Define the path to the file you want to hash here
    # Replace 'path/to/your/file.txt' with the actual path to your file
    # For demonstration, let's create a dummy file first
    dummy_file_name = "mi_archivo_para_hashear.txt"
    ruta_archivo_a_hashear = "//d250isinfs01\\qgis031\\TECNICA_INFRA\\test.docx"

    print(f"\nCalculando hash para el archivo: '{ruta_archivo_a_hashear}'")

    # Calculate the hash of the file
    hash_del_archivo = generar_hash_archivo(ruta_archivo_a_hashear, 'sha256')

    if hash_del_archivo:
        print(f"Hash SHA256 del archivo: {hash_del_archivo}")
    else:
        print("No se pudo calcular el hash del archivo.")


    print("\n--- Fin de la demostración ---")

    if hash_del_archivo == '14a5ec036eb484576ba7c32e6aa3d4859ae39a6a89d727060858912fea4dfba0':
        print(True)
    else:
        print(False)