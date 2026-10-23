# Análisis de la TRM de Colombia con Apache Spark (procesamiento batch)

Tarea 3 – Procesamiento de Datos con Apache Spark · Curso Big Data (202016911)
Universidad Nacional Abierta y a Distancia (UNAD)
Autora: Luisa Fernanda Fernández

## Problema
Analizar la evolución y la volatilidad de la Tasa Representativa del Mercado (TRM) en Colombia
para identificar tendencias, años de mayor variación y los días de mayores alzas y bajas.

## Conjunto de datos
- **Fuente:** Datos Abiertos Colombia (datos.gov.co), dataset de la TRM.
- **Archivo:** `rows.csv`, 8.368 registros desde diciembre de 1991 hasta octubre de 2026.
- **Columnas:** `VALOR` (pesos por dólar), `UNIDAD` (COP), `VIGENCIADESDE`, `VIGENCIAHASTA` (formato dd/MM/yyyy).

## Qué hace el código (`tarea3_batch.py`)
1. **Carga** el CSV con Spark DataFrames.
2. **Limpieza:** revisión de nulos, eliminación de duplicados, conversión de `VALOR` a número y de la fecha a tipo `date`.
3. **Transformación:** creación de las columnas `ANIO` y `MES`.
4. **EDA:** estadísticas generales, estadísticas por año (promedio, mínimo, máximo, desviación estándar),
   promedio mensual, variación diaria (valor y porcentaje) con una función de ventana, y conteo de días con TRM > 5.000.
5. **RDD:** ejemplos de `map`, `filter`, `reduceByKey`, `take`, `collect`, `count` y `reduce`.
6. **Visualización:** tres gráficas PNG (promedio mensual, promedio anual y volatilidad anual).

## Resultados principales
- Registros: 8.368 (sin nulos ni duplicados).
- TRM promedio histórica: $2.372,72; mínimo $620,62; máximo $5.061,21 (2022).
- Años de mayor volatilidad (desviación estándar): 2022 (387,65), 2023 (318,41) y 2015 (293,12).
- Solo 5 días con TRM superior a $5.000.
  
## Interpretación de los resultados
- **Tendencia de largo plazo:** la TRM pasó de unos $630 en 1991 a un promedio de $4.052 en 2025, con un máximo histórico de $5.061,21 en 2022. En la gráfica mensual se ve una tendencia al alza con periodos de fuerte aceleración.
- **Años de mayor volatilidad:** 2022 (desviación estándar de 387,65), 2023 (318,41) y 2015 (293,12). Son años con movimientos bruscos dentro del mismo año, no cambios graduales.
- **Posibles factores asociados:** el máximo histórico y la mayor volatilidad coinciden con 2022, año en que el Banco de la República (2022) describió un comportamiento volátil y una depreciación significativa del peso, explicada por factores externos e internos. Entre los externos, la alta inflación llevó a la Reserva Federal de EE. UU. a subir sus tasas de interés (Bancolombia, 2022). Entre los internos, el Banco de la República reconoció que la incertidumbre por la reforma tributaria elevó el dólar en octubre (Valora Analitik, 2022).
- - **Limitación:** este análisis es descriptivo; identifica cuándo hubo mayor variación, pero no demuestra causalidad.
  
## Requisitos
- Ubuntu 22.04 (máquina virtual), Java 11
- Apache Spark 3.5.8 (Scala 2.12.18) y Python 3.10
- `pip install matplotlib pandas`

## Cómo ejecutarlo
1. Descargar el dataset de la TRM desde datos.gov.co y guardarlo como `/home/vboxuser/tarea3/rows.csv`
   (si usa otra ruta, cambiar la variable `RUTA` en el script).
2. Ejecutar:
```bash
   cd ~/tarea3
   spark-submit tarea3_batch.py
```
3. Las gráficas se guardan en la misma carpeta:
   `grafica_mensual.png`, `grafica_anual.png`, `grafica_volatilidad.png`.

## Nota
La "variación diaria" compara registros consecutivos del archivo. Como la TRM de fines de semana
se publica con vigencia de varios días, algunas variaciones son entre periodos de vigencia.

## Referencias
Apache Software Foundation. (2026). *Apache Kafka* (Versión 3.9.2) [Software]. https://kafka.apache.org/

Apache Software Foundation. (2026). *Apache Spark* (Versión 3.5.8) [Software]. https://spark.apache.org/

Banco de la República. (2022). *Blog BanRep: El comportamiento de la tasa de cambio y las estrategias de política*. https://www.banrep.gov.co/es/noticias/blog-banrep-comportamiento-tasa-cambio-estrategias-politica

Bancolombia. (2022, 23 de agosto). *¿Cuál es la tendencia del dólar en Colombia 2022?* https://blog.bancolombia.com/actualidad/tendencia-dolar-colombia-2022/

Superintendencia Financiera de Colombia. (s. f.). *Tasa de Cambio Representativa del Mercado (TRM)* [Conjunto de datos]. Datos Abiertos Colombia. https://www.datos.gov.co/d/32sa-8pi3

Valora Analitik. (2022, 29 de noviembre). *BanRep: incertidumbre por gobierno Petro elevó dólar en Colombia en octubre*. https://www.valoraanalitik.com/banrep-incertidumbre-por-gobierno-petro-elevo-dolar-en-colombia-en-octubre/

