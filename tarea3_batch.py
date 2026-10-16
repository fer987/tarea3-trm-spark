from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import matplotlib
matplotlib.use("Agg")  # guarda las gráficas como PNG, sin ventana
import matplotlib.pyplot as plt

# 1. Sesión de Spark
spark = SparkSession.builder.appName("Tarea3_TRM_Batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Carga desde el archivo original (datos.gov.co)
RUTA = "file:///home/vboxuser/tarea3/rows.csv"
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(RUTA)

print("=== DATOS ORIGINALES ===")
print("Registros:", df.count())
df.printSchema()
df.show(5)

# 3. Limpieza
print("=== LIMPIEZA ===")
print("Nulos por columna:")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

df = df.dropDuplicates()
df = df.withColumn("VALOR", F.col("VALOR").cast("double"))

fecha_txt = F.substring(F.col("VIGENCIADESDE").cast("string"), 1, 10)
df = df.withColumn("FECHA", F.to_date(fecha_txt, "dd/MM/yyyy"))
df = df.dropna(subset=["VALOR", "FECHA"])
print("Registros después de limpiar:", df.count())

# 4. Transformaciones
df = df.withColumn("ANIO", F.year("FECHA")).withColumn("MES", F.month("FECHA"))

# 5. Análisis exploratorio (EDA)
print("=== ESTADÍSTICAS GENERALES ===")
df.select("VALOR").summary().show()

print("=== ESTADÍSTICAS POR AÑO ===")
por_anio = df.groupBy("ANIO").agg(
    F.round(F.avg("VALOR"), 2).alias("PROMEDIO"),
    F.round(F.min("VALOR"), 2).alias("MINIMO"),
    F.round(F.max("VALOR"), 2).alias("MAXIMO"),
    F.round(F.stddev("VALOR"), 2).alias("DESV_EST"),
    F.count("*").alias("REGISTROS")
).orderBy("ANIO")
por_anio.show(50)

print("=== PROMEDIO MENSUAL ===")
por_mes = df.groupBy("ANIO", "MES").agg(F.round(F.avg("VALOR"), 2).alias("PROMEDIO")) \
    .orderBy("ANIO", "MES")
por_mes.show(24)

print("=== VARIACIÓN DIARIA ===")
w = Window.orderBy("FECHA")
var = df.withColumn("VALOR_ANTERIOR", F.lag("VALOR").over(w)) \
    .withColumn("VARIACION", F.round(F.col("VALOR") - F.col("VALOR_ANTERIOR"), 2)) \
    .withColumn("VARIACION_PCT", F.round((F.col("VALOR") / F.col("VALOR_ANTERIOR") - 1) * 100, 2)) \
    .dropna(subset=["VARIACION"])

print("Mayores alzas:")
var.orderBy(F.col("VARIACION").desc()).select("FECHA", "VALOR", "VARIACION", "VARIACION_PCT").show(5)
print("Mayores bajas:")
var.orderBy(F.col("VARIACION").asc()).select("FECHA", "VALOR", "VARIACION", "VARIACION_PCT").show(5)

print("Días con TRM mayor a 5000:", df.filter(F.col("VALOR") > 5000).count())

# 6. Ejemplos con RDD
print("=== OPERACIONES CON RDD ===")
rdd = df.select("ANIO", "VALOR").rdd.map(lambda r: (r["ANIO"], r["VALOR"]))
print("Primeros 5 (take):", rdd.take(5))
print("Registros con valor > 5000 (filter + count):", rdd.filter(lambda x: x[1] > 5000).count())
registros_anio = rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
print("Registros por año (map + reduceByKey + collect):", sorted(registros_anio.collect()))
print("Valor máximo (reduce):", rdd.map(lambda x: x[1]).reduce(lambda a, b: max(a, b)))

# 7. Visualizaciones
mensual = por_mes.toPandas()
mensual["PERIODO"] = mensual["ANIO"].astype(str) + "-" + mensual["MES"].astype(str).str.zfill(2)
anual = por_anio.toPandas()

plt.figure(figsize=(12, 5))
plt.plot(mensual["PERIODO"], mensual["PROMEDIO"])
plt.xticks(mensual["PERIODO"][::6], rotation=45)
plt.title("TRM promedio mensual (COP por USD)")
plt.ylabel("Pesos colombianos")
plt.tight_layout()
plt.savefig("/home/vboxuser/tarea3/grafica_mensual.png")
plt.close()

plt.figure(figsize=(10, 5))
plt.bar(anual["ANIO"].astype(str), anual["PROMEDIO"])
plt.title("TRM promedio por año")
plt.ylabel("Pesos colombianos")
plt.tight_layout()
plt.savefig("/home/vboxuser/tarea3/grafica_anual.png")
plt.close()

plt.figure(figsize=(10, 5))
plt.bar(anual["ANIO"].astype(str), anual["DESV_EST"], color="darkorange")
plt.title("Volatilidad de la TRM por año (desviación estándar)")
plt.ylabel("Pesos colombianos")
plt.tight_layout()
plt.savefig("/home/vboxuser/tarea3/grafica_volatilidad.png")
plt.close()

print("Gráficas guardadas en /home/vboxuser/tarea3/")
spark.stop()
