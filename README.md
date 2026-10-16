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
