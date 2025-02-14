#Tras analizar la estructura de la base de datos fuente, se observa que, en los registros anteriores a 2009,
#los nombres de las columnas incluyen los prefijos "EL" para el cónyuge hombre y "LA" para la cónyuge mujer. 
#Esto se debe a la introducción de registros de parejas del mismo sexo a partir de ese año. Por ello, se renombraron 
#las columnas en los primeros archivos, considerando que cuando sexo_con1 != sexo_con2, el valor "2" siempre corresponde a una mujer.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil
import glob
import pandas as pd
import pyarrow
#!pip install pyarrow

# Iniciar sesión Spark
spark = SparkSession.builder.appName("MAT_INF_ADO").getOrCreate()

# Ruta de archivos en Google Drive
drive_path = "/content/drive/MyDrive/Matrimonios/"

# Diccionario de renombrado para los archivos 2003-2009
rename_dict = {
    "EDAD_EL": "EDAD_CON1", 
    "OCUP_EL": "OCUP_CON1", 
    "ESCOL_EL": "CONACTCON1", 
    "CON_ACT_EL": "SITLABCON1",
    "EDAD_LA": "EDAD_CON2", 
    "OCUP_LA": "OCUP_CON2", 
    "ESCOL_LA": "CONACTCON2", 
    "CON_ACT_LA": "SITLABCON2"
}

# Columnas seleccionadas para archivos 2010-2023
selected_columns = [
    "ENT_REGIS", "ANIO_REGIS", "EDAD_CON1", "OCUP_CON1", "CONACTCON1", 
    "SITLABCON1", "EDAD_CON2", "OCUP_CON2", "CONACTCON2", "SITLABCON2"
]

def cargar_procesar_archivos(files, renombrar=False):
    """Carga y procesa los archivos especificados, manejando errores y optimizando lectura."""
    df_list = []
    
    for file in files:
        file_path = os.path.join(drive_path, file)
        
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=False, multiLine=True)
            
            if renombrar:
                df = df.selectExpr(*[f"{k} as {v}" for k, v in rename_dict.items()], "ENT_REGIS", "ANIO_REGIS")
            else:
                df = df.select(*selected_columns)
            
            df_filtered = df.filter((col("EDAD_CON2") < 18) & (col("ANIO_REGIS") > 2002))
            df_list.append(df_filtered)
        
        except Exception as e:
            print(f"⚠️ Advertencia: No se pudo procesar {file}. Error: {str(e)}")
    
    if df_list:
        from functools import reduce
        return reduce(lambda df1, df2: df1.unionByName(df2), df_list)
    else:
        return None

# Procesar archivos 2003-2009 con renombrado
files_03_09 = ["MATRI09.csv", "MATRI08.csv", "MATRI07.csv", "MATRI06.csv", "MATRI05.csv", "MATRI04.csv", "MATRI03.csv"]
df03_09 = cargar_procesar_archivos(files_03_09, renombrar=True)

# Procesar archivos 2010-2023 sin renombrado
files_10_23 = [
    "conjunto_de_datos_emat2023.csv", "conjunto_de_datos_matrimonios_2022.csv",
    "conjunto_de_datos_matrimonios_2021.csv", "conjunto_de_datos_matrimonios_2020.csv",
    "conjunto_de_datos_matrimonios_2019.CSV", "conjunto_de_datos_matrimonios_2018.CSV",
    "conjunto_de_datos_matrimonios_2017.csv", "MATRI16.csv", "MATRI15.csv", "MATRI14.csv",
    "MATRI13.csv", "MATRI12.csv", "MATRI11.csv", "MATRI10.csv"
]
df10_23 = cargar_procesar_archivos(files_10_23)

# Unir ambos DataFrames si existen
if df03_09 and df10_23:
    df_final = df03_09.unionByName(df10_23)
elif df03_09:
    df_final = df03_09
elif df10_23:
    df_final = df10_23
else:
    raise ValueError("No se pudo cargar ningún archivo.")

# Reparticionar antes de escribir para optimizar escritura
df_final = df_final.repartition(1)

# Guardar en formato Parquet temporalmente (mejor rendimiento que CSV)
output_dir = "/content/drive/MyDrive/Matrimonios/MAT_INF_ADO_temp"
df_final.write.mode("overwrite").parquet(output_dir)

# Renombrar archivo final de Parquet
final_parquet_name = "MAT_INF_ADO.parquet"
final_parquet_path = os.path.join(drive_path, final_parquet_name)

# Buscar archivo generado dentro de la carpeta temporal
parquet_files = glob.glob(os.path.join(output_dir, "*.parquet"))
if parquet_files:
    temp_parquet_file = parquet_files[0]  # Tomar el primer archivo generado
    shutil.move(temp_parquet_file, final_parquet_path)
else:
    raise FileNotFoundError("No se generó ningún archivo Parquet.")

# Eliminar carpeta temporal
shutil.rmtree(output_dir)

# Detener Spark
spark.stop()

print("✅ ETL finalizado exitosamente. Archivo Parquet guardado en:", final_parquet_path)

df = pd.read_parquet('/content/drive/MyDrive/Matrimonios/MAT_INF_ADO.parquet')

# Guardar CSV file
df.to_csv('/content/drive/MyDrive/Matrimonios/MAT_INF_ADO.csv', index=False) 
