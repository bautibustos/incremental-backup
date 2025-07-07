import hashlib
import os

def generar_hash_archivo(ruta_archivo, chunk_size=4096):
    """
    Genera un hash del contenido de un archivo usando el algoritmo especificado.
    Lee el archivo en bloques para manejar archivos grandes eficientemente.

    Args:
        ruta_archivo (str): La ruta del archivo al que se le calculará el hash.
        chunk_size (int): Tamaño de los bloques a leer del archivo (en bytes).

    Returns:
        str: El hash hexadecimal del contenido del archivo, o None si hay un error.
    """
    hasher = hashlib.sha256()
    
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
    ruta_archivo_a_hashear = "//d250isinfs01\\qgis031\\TECNICA_INFRA\\test.docx"

    print(f"\nCalculando hash para el archivo: '{ruta_archivo_a_hashear}'")

    # Calculate the hash of the file
    hash_del_archivo = generar_hash_archivo(ruta_archivo_a_hashear)

    if hash_del_archivo:
        print(f"Hash SHA256 del archivo: {hash_del_archivo}")
    else:
        print("No se pudo calcular el hash del archivo.")