# Tarea 3 – Procesamiento de datos con Apache Spark: ventas de una tienda en línea (Online Retail II)

**Curso:** Big Data (código 202016911) – Universidad Nacional Abierta y a Distancia (UNAD)
**Autora:** Angie Nataly Tellez Vargas  |  **Grupo:** 202016911_66

## 1. Problema

> ¿Qué productos y países generan más ingresos, cómo varían las ventas mes a mes y qué tan frecuentes son los clientes, para apoyar decisiones de marketing de una tienda en línea?

Se analiza el histórico de ventas de una tienda en línea con Apache Spark (PySpark) sobre HDFS: limpieza de datos, análisis exploratorio (EDA), ejemplo con RDDs y visualización de resultados.

## 2. Dataset

- **Nombre:** Online Retail II
- **Fuente original:** UCI Machine Learning Repository – https://archive.ics.uci.edu/dataset/502/online+retail+ii
- **Licencia:** CC BY 4.0
- **Contenido:** 1.067.371 transacciones de una tienda en línea del Reino Unido, del 01/12/2009 al 09/12/2011. Moneda: libras esterlinas (£).
- **Formato original:** un archivo Excel (`online_retail_II.xlsx`, 43,5 MB) con dos hojas: `Year 2009-2010` y `Year 2010-2011`.
- **Columnas:** `Invoice`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `Price`, `Customer ID`, `Country`.

El dataset **no se incluye** en este repositorio por su tamaño. Debe descargarse desde el enlace de la fuente original.

### Conversión a CSV
Spark lee CSV de forma nativa pero no archivos `.xlsx`, así que el Excel se convierte (sin modificar valores) a dos CSV, uno por hoja, con `convertir_xlsx_a_csv.py`:

```bash
pip install pandas openpyxl
python3 convertir_xlsx_a_csv.py online_retail_II.xlsx ./salida
```

Genera `online_retail_2009_2010.csv` y `online_retail_2010_2011.csv` (fechas en formato `yyyy-MM-dd HH:mm:ss`).

## 3. Entorno utilizado

- Máquina virtual Ubuntu 22.04 (VirtualBox), configurada en la Tarea 2
- Hadoop/HDFS en un solo nodo
- Apache Spark 3.5.8 (Scala 2.12.18), OpenJDK 11, Python 3.10
- Bibliotecas de Python: `pyspark`, `matplotlib`

## 4. Estructura del repositorio

| Archivo | Descripción |
|---|---|
| `convertir_xlsx_a_csv.py` | Convierte el Excel original en dos CSV |
| `batch_retail.py` | Carga, limpieza, transformación y análisis exploratorio con DataFrames |
| `retail_rdd_graficas.py` | Ejemplo con RDDs y generación de gráficas (reutiliza funciones de `batch_retail.py`) |
| `graficas/` | Gráficas generadas (PNG) |
| `resultados/` | Salida de consola de cada ejecución |

## 5. Cómo ejecutarlo

1. Iniciar HDFS (`start-all.sh` como usuario de Hadoop) y subir los CSV:
   ```bash
   hdfs dfs -mkdir -p /Tarea3/retail
   hdfs dfs -put online_retail_2009_2010.csv online_retail_2010_2011.csv /Tarea3/retail/
   ```
2. Instalar matplotlib: `pip install matplotlib`
3. Análisis con DataFrames:
   ```bash
   spark-submit batch_retail.py 2>/dev/null | tee resultados_batch.txt
   ```
4. RDDs y gráficas (deja los PNG en `graficas/`):
   ```bash
   spark-submit retail_rdd_graficas.py 2>/dev/null | tee resultados_rdd.txt
   ```

Los dos scripts deben estar en la misma carpeta. La ruta de HDFS está en la constante `RUTA_HDFS` de `batch_retail.py`.

## 6. Qué hace el código

**Limpieza (con resumen de filas eliminadas en cada regla):**

| Paso | Filas |
|---|---|
| Filas iniciales (2 CSV) | 1.067.371 |
| Tras eliminar duplicados exactos (incluye el solapamiento del 1 al 9 de dic. de 2010 entre las dos hojas) | 1.033.036 |
| Cancelaciones (facturas que empiezan con "C"), separadas | 19.104 |
| Ventas sin cancelaciones | 1.013.932 |
| Tras exigir `Quantity > 0` y `Price > 0` | 1.007.913 |
| Tras excluir códigos que no son productos (envíos, comisiones, pruebas, vales) | 1.003.340 |

Además: se normaliza el texto (`trim`, `upper`), y se configura la lectura del CSV con `escape` de comilla doble porque las descripciones con comillas vienen duplicadas (`""`).

**Análisis (DataFrames):** indicadores generales, top de productos por ingresos brutos, unidades e ingresos netos (ventas menos devoluciones), ingresos por país (con y sin Reino Unido), tendencia mensual, clientes más frecuentes y cancelaciones.

**RDDs:** `map`, `filter`, `flatMap`, `reduceByKey`, `sortBy`, y las acciones `count`, `take` y `reduce`.

## 7. Resultados principales

| Indicador | Valor |
|---|---|
| Ingresos totales | £ 19.643.861,62 |
| Facturas | 39.516 |
| Ticket promedio por factura | £ 497,11 |
| Productos distintos | 4.724 |
| Clientes identificados | 5.852 |
| Países | 43 |
| Participación del Reino Unido en los ingresos | 85,53 % |
| Valor devuelto sobre ingresos | 3,65 % |
| Ingresos netos (ventas − devoluciones) | £ 18.927.435,65 |
| Clientes con una sola factura | 1.618 de 5.852 (27,6 %) |

Gráficas: `graficas/1_ingresos_mensuales.png`, `graficas/2_top_productos_netos.png`, `graficas/3_top_paises_sin_uk.png`.

**Observaciones:**
- Dos de los diez productos con más ingresos brutos (`23843` y `23166`) corresponden a pedidos muy grandes que fueron cancelados casi por completo, por eso el ranking se calcula también con ingresos netos.
- Los ingresos son más altos en octubre y noviembre de ambos años. Es un patrón observado en los datos; sus causas no se pueden confirmar con este dataset.
- Casi el 28 % de los clientes identificados compró una sola vez.

## 8. Limitaciones

- Los datos son de 2009 a 2011 y de un solo comercio del Reino Unido; los resultados no se generalizan a otros mercados.
- El 22,6 % de las líneas de venta limpias no tiene `Customer ID`; esos registros se usan en los análisis de productos y países, pero no en los de clientes.
- Diciembre de 2011 es un mes parcial (llega solo hasta el 9).

## 9. Procesamiento en tiempo real (Kafka + Spark Streaming)

Se desarrolla en una parte aparte de la tarea siguiendo el Anexo 3. Se agregará a este repositorio.
