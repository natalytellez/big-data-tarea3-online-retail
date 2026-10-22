"""
batch_retail.py
Procesamiento en batch del dataset Online Retail II con Apache Spark.

Fuente de datos: UCI Machine Learning Repository - Online Retail II (CC BY 4.0)
Los datos están en HDFS: /Tarea3/retail/ (un CSV por hoja del Excel original).
Moneda: libras esterlinas (GBP).
"""

from pyspark.sql import SparkSession, functions as F

# Carpeta de HDFS donde están los dos CSV
RUTA_HDFS = "hdfs://localhost:9000/Tarea3/retail/"

# StockCode que NO corresponden a productos reales (envíos, comisiones, ajustes,
# pruebas). Se excluyen para que no contaminen el ranking de productos.
CODIGOS_NO_PRODUCTO = [
    "POST", "DOT", "M", "C2", "D", "S", "B", "CRUK", "ADJUST", "ADJUST2",
    "BANK CHARGES", "AMAZONFEE", "PADS", "TEST001", "TEST002",
]


def crear_spark():
    """Crea la sesión de Spark. Usa 8 particiones en los shuffles (el valor por
    defecto, 200, es excesivo para un dataset de este tamaño en una sola máquina)."""
    spark = (
        SparkSession.builder
        .appName("BatchRetail")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def cargar_datos(spark):
    """Lee todos los CSV de la carpeta de HDFS en un solo DataFrame."""
    df = (
        spark.read
        .option("header", "true")        # la primera línea son los nombres de columna
        .option("inferSchema", "true")   # Spark detecta el tipo de cada columna
        .option("escape", '"')
        .csv(RUTA_HDFS)
    )
    # "Customer ID" tiene un espacio en el nombre: lo renombramos
    return df.withColumnRenamed("Customer ID", "CustomerID")


def contar_nulos(df):
    """Devuelve un DataFrame de una fila con la cantidad de nulos de cada columna."""
    return df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]
    )


def es_no_producto():
    """Condición que identifica los códigos que no son productos (envíos, pruebas...)."""
    return (
        F.col("StockCode").isin(CODIGOS_NO_PRODUCTO)
        | F.col("StockCode").startswith("GIFT_")
    )


def limpiar_datos(df):
    """
    Limpia el DataFrame y devuelve (df_ventas, df_cancelaciones).
    Muestra cuántas filas elimina cada regla.
    """
    n0 = df.count()

    # 1. Duplicados exactos (incluye el solapamiento del 1 al 9 de dic. de 2010
    #    que está en las dos hojas del Excel). cache() guarda el resultado en
    #    memoria para no recalcularlo en cada count().
    df1 = df.dropDuplicates().cache()
    n1 = df1.count()

    # 2. Normalizar texto: quitar espacios sobrantes y unificar mayúsculas
    df2 = (
        df1
        .withColumn("StockCode", F.upper(F.trim("StockCode")))
        .withColumn("Description", F.trim("Description"))
        .withColumn("Country", F.trim("Country"))
    )

    # 3. Separar cancelaciones (facturas que empiezan con "C"): se guardan aparte
    es_cancelacion = F.col("Invoice").startswith("C")
    df_cancelaciones = df2.filter(es_cancelacion)
    df3 = df2.filter(~es_cancelacion)
    n_cancel = df_cancelaciones.count()
    n3 = df3.count()

    # 4. Ventas reales: cantidad y precio deben ser positivos
    df4 = df3.filter((F.col("Quantity") > 0) & (F.col("Price") > 0))
    n4 = df4.count()

    # 5. Excluir códigos que no son productos (envíos, comisiones, pruebas, vales)
    df_ventas = df4.filter(~es_no_producto()).cache()
    n5 = df_ventas.count()

    print("\n=== RESUMEN DE LIMPIEZA ===")
    print(f"Filas iniciales (2 CSV):               {n0:>10,}")
    print(f"Tras quitar duplicados exactos:        {n1:>10,}  (-{n0 - n1:,})")
    print(f"Cancelaciones separadas:               {n_cancel:>10,}")
    print(f"Ventas sin cancelaciones:              {n3:>10,}")
    print(f"Tras exigir Quantity>0 y Price>0:      {n4:>10,}  (-{n3 - n4:,})")
    print(f"Tras quitar códigos no producto:       {n5:>10,}  (-{n4 - n5:,})")
    return df_ventas, df_cancelaciones


def transformar_datos(df_ventas):
    """Crea columnas derivadas que usaremos en los análisis."""
    return (
        df_ventas
        # Ingreso de cada línea de factura (en libras)
        .withColumn("Revenue", F.col("Quantity") * F.col("Price"))
        # Mes de la venta, para analizar tendencias (ej. 2010-12)
        .withColumn("YearMonth", F.date_format("InvoiceDate", "yyyy-MM"))
        # Hora del día de la venta (0 a 23)
        .withColumn("Hour", F.hour("InvoiceDate"))
    )


def mostrar_kpis(df):
    """Indicadores generales del negocio sobre las ventas limpias."""
    fila = df.agg(
        F.sum("Revenue").alias("ingresos"),
        F.countDistinct("Invoice").alias("facturas"),
        F.countDistinct("StockCode").alias("productos"),
        F.countDistinct("CustomerID").alias("clientes"),   # ignora los nulos
        F.countDistinct("Country").alias("paises"),
    ).first()
    print("\n=== INDICADORES GENERALES (GBP) ===")
    print(f"Ingresos totales:            {fila['ingresos']:>14,.2f}")
    print(f"Facturas distintas:          {fila['facturas']:>14,}")
    print(f"Ticket promedio por factura: {fila['ingresos'] / fila['facturas']:>14,.2f}")
    print(f"Productos distintos:         {fila['productos']:>14,}")
    print(f"Clientes identificados:      {fila['clientes']:>14,}")
    print(f"Países:                      {fila['paises']:>14,}")
    return fila["ingresos"], fila["facturas"]


def top_productos(df, por="Ingresos", n=10):
    """Top N productos ordenados por 'Ingresos' o por 'Unidades'."""
    return (
        df.groupBy("StockCode")
        .agg(
            F.sum("Quantity").alias("Unidades"),
            F.round(F.sum("Revenue"), 2).cast("decimal(14,2)").alias("Ingresos"),
            F.max("Description").alias("Descripcion"),
        )
        .orderBy(F.desc(por))
        .limit(n)
    )


def ingresos_por_pais(df, excluir_uk=False):
    """Facturas, ingresos y porcentaje por país. Opcionalmente sin Reino Unido."""
    base = df.filter(F.col("Country") != "United Kingdom") if excluir_uk else df
    total = base.agg(F.sum("Revenue")).first()[0]
    return (
        base.groupBy("Country")
        .agg(
            F.countDistinct("Invoice").alias("Facturas"),
            F.round(F.sum("Revenue"), 2).cast("decimal(14,2)").alias("Ingresos"),
        )
        .withColumn("Porcentaje", F.round(F.col("Ingresos") / F.lit(total) * 100, 2))
        .orderBy(F.desc("Ingresos"))
    )


def tendencia_mensual(df):
    """Facturas e ingresos por mes, en orden cronológico."""
    return (
        df.groupBy("YearMonth")
        .agg(
            F.countDistinct("Invoice").alias("Facturas"),
            F.round(F.sum("Revenue"), 2).cast("decimal(14,2)").alias("Ingresos"),
        )
        .orderBy("YearMonth")
    )


def clientes_frecuentes(df, n=10):
    """Top N clientes por número de facturas (solo clientes identificados)."""
    return (
        df.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(
            F.countDistinct("Invoice").alias("Facturas"),
            F.round(F.sum("Revenue"), 2).cast("decimal(14,2)").alias("Ingresos"),
            F.max("Country").alias("Pais"),
        )
        .orderBy(F.desc("Facturas"), F.desc("Ingresos"))
        .limit(n)
    )


def clientes_una_compra(df):
    """Cuántos clientes identificados compraron una sola vez."""
    por_cliente = (
        df.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID")
        .agg(F.countDistinct("Invoice").alias("Facturas"))
    )
    total = por_cliente.count()
    una = por_cliente.filter(F.col("Facturas") == 1).count()
    return una, total


def preparar_devoluciones(df_cancelaciones):
    """Cancelaciones de productos reales: cantidad y valor quedan en negativo."""
    return (
        df_cancelaciones
        .filter(~es_no_producto() & (F.col("Price") > 0))
        .withColumn("Revenue", F.col("Quantity") * F.col("Price"))
    )


def resumen_cancelaciones(devol, ingresos_totales, facturas_venta):
    """Indicadores de cancelaciones y los productos más devueltos."""
    fila = devol.agg(
        F.countDistinct("Invoice").alias("facturas"),
        F.sum("Quantity").alias("unidades"),
        F.sum("Revenue").alias("valor"),
    ).first()
    print("\n=== ANÁLISIS 5A: CANCELACIONES (GBP) ===")
    print(f"Facturas de cancelación:             {fila['facturas']:>12,}")
    print(f"Unidades devueltas:                  {abs(fila['unidades']):>12,}")
    print(f"Valor devuelto:                      {abs(fila['valor']):>12,.2f}")
    print(f"Valor devuelto sobre ingresos:       {abs(fila['valor']) / ingresos_totales * 100:>11.2f}%")
    print(f"Cancelaciones por cada 100 ventas:   {fila['facturas'] / facturas_venta * 100:>12.2f}")
    return fila["valor"]


def productos_mas_devueltos(devol, n=5):
    """Productos con más unidades devueltas (cantidades negativas: orden ascendente)."""
    return (
        devol.groupBy("StockCode")
        .agg(
            F.sum("Quantity").alias("Unidades"),
            F.round(F.sum("Revenue"), 2).cast("decimal(14,2)").alias("Valor"),
            F.max("Description").alias("Descripcion"),
        )
        .orderBy("Unidades")
        .limit(n)
    )


def top_productos_netos(df, devol, n=10):
    """Top N productos por ingreso NETO: ventas menos devoluciones."""
    ventas = df.select("StockCode", "Description", "Revenue")
    devoluciones = devol.select("StockCode", "Description", "Revenue")
    return (
        ventas.unionByName(devoluciones)
        .groupBy("StockCode")
        .agg(
            F.round(F.sum("Revenue"), 2).cast("decimal(14,2)").alias("IngresosNetos"),
            F.max("Description").alias("Descripcion"),
        )
        .orderBy(F.desc("IngresosNetos"))
        .limit(n)
    )


def main():
    spark = crear_spark()
    df = cargar_datos(spark)

    print("\n=== ESQUEMA ===")
    df.printSchema()

    print("=== NULOS POR COLUMNA (ANTES DE LIMPIAR) ===")
    contar_nulos(df).show(truncate=False)

    df_ventas, df_cancelaciones = limpiar_datos(df)

    print("\n=== NULOS POR COLUMNA (DESPUÉS DE LIMPIAR) ===")
    contar_nulos(df_ventas).show(truncate=False)

    # --- Transformación y análisis exploratorio (EDA) ---
    df_t = transformar_datos(df_ventas)
    ingresos_totales, facturas_venta = mostrar_kpis(df_t)

    print("\n=== ANÁLISIS 1A: TOP 10 PRODUCTOS POR INGRESOS BRUTOS ===")
    top_productos(df_t, por="Ingresos", n=10).show(truncate=False)

    print("=== ANÁLISIS 1B: TOP 5 PRODUCTOS POR UNIDADES VENDIDAS ===")
    top_productos(df_t, por="Unidades", n=5).show(truncate=False)

    print("=== ANÁLISIS 2A: TOP 10 PAÍSES POR INGRESOS ===")
    ingresos_por_pais(df_t).show(10, truncate=False)

    print("=== ANÁLISIS 2B: TOP 10 PAÍSES SIN REINO UNIDO ===")
    ingresos_por_pais(df_t, excluir_uk=True).show(10, truncate=False)

    print("=== ANÁLISIS 3: TENDENCIA MENSUAL DE VENTAS ===")
    tendencia_mensual(df_t).show(30, truncate=False)

    print("=== ANÁLISIS 4A: TOP 10 CLIENTES POR FRECUENCIA DE COMPRA ===")
    clientes_frecuentes(df_t).show(truncate=False)
    una, total = clientes_una_compra(df_t)
    print(f"Clientes con una sola factura: {una:,} de {total:,} ({una / total * 100:.1f}%)")

    devol = preparar_devoluciones(df_cancelaciones)
    valor_devuelto = resumen_cancelaciones(devol, ingresos_totales, facturas_venta)

    print("\n=== ANÁLISIS 5B: TOP 5 PRODUCTOS MÁS DEVUELTOS ===")
    productos_mas_devueltos(devol).show(truncate=False)

    print("=== ANÁLISIS 1C: TOP 10 PRODUCTOS POR INGRESOS NETOS (VENTAS - DEVOLUCIONES) ===")
    top_productos_netos(df_t, devol).show(truncate=False)
    print(f"Ingresos netos totales: {ingresos_totales + valor_devuelto:,.2f}")

    spark.stop()


if __name__ == "__main__":
    main()
