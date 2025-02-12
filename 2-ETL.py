#Tras analizar la estructura de la base de datos fuente, se observa que, en los registros anteriores a 2009, los nombres de las columnas incluyen los prefijos "EL" para el cónyuge hombre y "LA" para la cónyuge mujer. Esto se debe a la introducción de registros de parejas del mismo sexo a partir de ese año. Por ello, se renombraron las columnas en los primeros archivos, considerando que cuando sexo_con1 != sexo_con2, el valor "2" siempre corresponde a una mujer.

#Librerias necesarias
#!pip install pyspark #En notebook
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Iniciar sesion SPARK
spark = SparkSession.builder.appName("MAT_INF_ADO").getOrCreate()
drive_path = "/content/drive/MyDrive/Matrimonios/" 

#---------------------------------------------------------------------------------------------------------------2003-2009

files = ["MATRI09.csv", "MATRI08.csv", "MATRI07.csv", "MATRI06.csv", "MATRI05.csv", "MATRI03.csv", "MATRI04.csv"]

#Renombrar columnas
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

df_list = []

for file in files:
    file_path = drive_path + file  # Ruta completa
    df = spark.read.csv(file_path, header=True, inferSchema=True)  # Leer CSV con cabecera e inferencia de tipos
    
    # Seleccionar solo las columnas necesarias
    selected_cols = list(rename_dict.keys()) + ["ENT_REGIS", "ANIO_REGIS"]
    df = df.select([col(c) for c in selected_cols])  
    # Renombrar columnas
    for old_name, new_name in rename_dict.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    # Filtrar los datos según la condición
    df_filtered = df.filter((col("EDAD_CON2") < 20) & (col("ANIO_REGIS") > 2002))
    
    df_list.append(df_filtered)

#Unir todos los DataFrames

final_df = df_list[0]
for df in df_list[1:]:
    df03_09 = final_df.union(df)

#---------------------------------------------------------------------------------------------------------------2010-2023
files_10_23 = [
    "MATRI10.csv", "MATRI11.csv", "MATRI12.csv", "MATRI13.csv", "MATRI14.csv", 
    "MATRI15.csv", "MATRI16.csv", "conjunto_de_datos_matrimonios_2022.csv", 
    "conjunto_de_datos_matrimonios_2021.csv", "conjunto_de_datos_matrimonios_2020.csv", 
    "conjunto_de_datos_matrimonios_2019.CSV"
]

selected_columns = [
    "ENT_REGIS", "ANIO_REGIS", "EDAD_CON1", "OCUP_CON1", "CONACTCON1", "SITLABCON1",
    "EDAD_CON2", "OCUP_CON2", "CONACTCON2", "SITLABCON2"
]

# Leer y procesar los archivos
df_list2 = []
for file in files_10_23:
    file_path = drive_path + file  
    df = spark.read.csv(file_path, header=True, inferSchema=True)  
    df = df.select([col(c) for c in selected_columns]) # Seleccionar solo las columnas necesarias
    df_filtered = df.filter(
        (col("EDAD_CON1") != col("EDAD_CON2")) & 
        (col("EDAD_CON2") < 20) & 
        (col("anio_regis") > 2002)
    )# Aplicar los filtros
    df_list2.append(df_filtered)

#Unir todos los DataFrames en df10_23
df10_23 = df_list2[0]
for df in df_list2[1:]:
    df10_23 = df10_23.union(df)

# Unir los DataFrames df10_23 y df03_09
MAT_INF_ADO = df10_23.union(df03_09)

# Guardar el DataFrame unido en un archivo CSV
MAT_INF_ADO.write.csv("/content/drive/MyDrive/Matrimonios/MAT_INF_ADO.csv", header=True)

spark.stop()
