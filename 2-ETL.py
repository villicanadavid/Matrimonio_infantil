#Tras analizar la estructura de la base de datos fuente, se observa que, en los registros anteriores a 2009, los nombres de las columnas incluyen los prefijos "EL" para el cónyuge hombre y "LA" para la cónyuge mujer. Esto se debe a la introducción de registros de parejas del mismo sexo a partir de ese año. Por ello, se renombraron las columnas en los primeros archivos, considerando que cuando sexo_con1 != sexo_con2, el valor "2" siempre corresponde a una mujer.

#Librerias necesarias
#!pip install pyspark #En notebook
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#Iniciar sesion SPARK
spark = SparkSession.builder.appName("MAT_INF_ADO").getOrCreate()

# Función para cargar y procesar archivos (DF09-03)
def procesar_archivos_09_03(archivos):
    df_lista = []
    for archivo in archivos:
        df = spark.read.csv(ruta_archivos + archivo, header=True, inferSchema=True)
        # Renombrar columnas
        df = df.withColumnRenamed("EDAD_EL", "EDAD_CON1") \
               .withColumnRenamed("OCUP_EL", "OCUP_CON1") \
               .withColumnRenamed("ESCOL_EL", "CONACTCON1") \
               .withColumnRenamed("CON_ACT_EL", "SITLABCON1") \
               .withColumnRenamed("EDAD_LA", "EDAD_CON2") \
               .withColumnRenamed("OCUP_LA", "OCUP_CON2") \
               .withColumnRenamed("ESCOL_LA", "CONACTCON2") \
               .withColumnRenamed("CON_ACT_LA", "SITLABCON2")

        # Seleccionar columnas y aplicar filtro
        df = df.select("ENT_REGIS", "ANIO_REGIS", "EDAD_CON1", "OCUP_CON1", "CONACTCON1", "SITLABCON1", "EDAD_CON2", "OCUP_CON2", "CONACTCON2", "SITLABCON2") \
               .filter((col("EDAD_CON2") < 20) & (col("ANIO_REGIS") > 2002))
        df_lista.append(df)
    return df_lista

# Función para cargar y procesar archivos (DF10-23)
def procesar_archivos_10_23(archivos):
    df_lista = []
    for archivo in archivos:
        df = spark.read.csv(ruta_archivos + archivo, header=True, inferSchema=True)
        # Seleccionar columnas y aplicar filtro
        df = df.select("ent_regis", "anio_regis", "EDAD_CON1", "OCUP_CON1", "CONACTCON1", "SITLABCON1", "EDAD_CON2", "OCUP_CON2", "CONACTCON2", "SITLABCON2") \
               .filter((col("EDAD_CON1") != col("EDAD_CON2")) & (col("EDAD_CON2") < 20) & (col("ANIO_REGIS") > 2002))
        df_lista.append(df)
    return df_lista


# Lista de archivos para DF09-03
archivos_09_03 = ["MATRI09.csv", "MATRI08.csv", "MATRI07.csv", "MATRI06.csv", "MATRI05.csv", "MATRI03.csv", "MATRI04.csv"]

# Lista de archivos para DF10-23
archivos_10_23 = ["MATRI10.csv", "MATRI11.csv", "MATRI12.csv", "MATRI13.csv", "MATRI14.csv", "MATRI15.csv", "MATRI16.csv", "conjunto_de_datos_matrimonios_2022.csv", "conjunto_de_datos_matrimonios_2021.csv", "conjunto_de_datos_matrimonios_2020.csv", "conjunto_de_datos_matrimonios_2019.CSV"]

# Procesar archivos y crear DataFrames (FINALLY CORRECTED)
dfs_09_03 = procesar_archivos_09_03(archivos_09_03)
df_09_03 = dfs_09_03[0] if len(dfs_09_03) == 1 else reduce(lambda df1, df2: df1.union(df2), dfs_09_03) if len(dfs_09_03) > 0 else spark.createDataFrame([], ["ENT_REGIS", "ANIO_REGIS", "EDAD_CON1", "OCUP_CON1", "CONACTCON1", "SITLABCON1", "EDAD_CON2", "OCUP_CON2", "CONACTCON2", "SITLABCON2"])


dfs_10_23 = procesar_archivos_10_23(archivos_10_23)
df_10_23 = dfs_10_23[0] if len(dfs_10_23) == 1 else reduce(lambda df1, df2: df1.union(df2), dfs_10_23) if len(dfs_10_23) > 0 else spark.createDataFrame([], ["ent_regis", "anio_regis", "EDAD_CON1", "OCUP_CON1", "CONACTCON1", "SITLABCON1", "EDAD_CON2", "OCUP_CON2", "CONACTCON2", "SITLABCON2"])


# Unir los DataFrames (DF09-03 y DF10-23)
columnas_comunes = list(set(df_09_03.columns) & set(df_10_23.columns))  # Obtener las columnas comunes
MAT_INF_ADO = df_09_03.select(*columnas_comunes).union(df_10_23.select(*columnas_comunes))

spark.stop()
