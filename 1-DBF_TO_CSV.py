#Primer Script para transformacion de archivos desde la fuente origen: https://www.inegi.org.mx/programas/emat/#microdatos
#Es necesario convertir algunas DB de la fuente origen a CSV
!pip install dbfread
import os
import pandas as pd
from dbfread import DBF

# Ruta a la carpeta con los archivos .DBF
ruta_carpeta = "/content/drive/MyDrive/Matrimonios"

# Recorrer todos los archivos en la carpeta
for archivo in os.listdir(ruta_carpeta):
    if archivo.lower().endswith(('.dbf', '.DBF')):
        ruta_completa = os.path.join(ruta_carpeta, archivo)
        try:
            # Leer el archivo .DBF usando dbfread
            dbf = DBF(ruta_completa)
            # Convertir a DataFrame de pandas
            df = pd.DataFrame(iter(dbf))
            # Crear el nombre del archivo CSV (misma base que el .DBF)
            nombre_csv = os.path.splitext(archivo)[0] + ".csv"
            ruta_csv = os.path.join(ruta_carpeta, nombre_csv)
            # Guardar el DataFrame como CSV
            df.to_csv(ruta_csv, index=False, encoding='utf-8') # encoding='latin-1' si tienes problemas con caracteres especiales
            print(f"Archivo '{archivo}' convertido a '{nombre_csv}' exitosamente.")
        except Exception as e:
            print(f"Error al procesar el archivo '{archivo}': {e}")
