"""
retail_rdd_graficas.py
Parte 2 del procesamiento batch de Online Retail II:
  - Ejemplo con RDDs (map, filter, flatMap, reduceByKey, take, count, reduce).
  - Gráficas de los resultados (se guardan como PNG en la carpeta "graficas").

Reutiliza las funciones de limpieza y análisis de batch_retail.py (mismo directorio).
Ejecución:  spark-submit retail_rdd_graficas.py 2>/dev/null | tee resultados_rdd.txt
"""

import os
from operator import add

import matplotlib
matplotlib.use("Agg")                 # modo sin pantalla: solo guarda archivos
import matplotlib.pyplot as plt

from batch_retail import (
    crear_spark, cargar_datos, limpiar_datos, transformar_datos,
    tendencia_mensual, ingresos_por_pais, preparar_devoluciones,
    top_productos_netos,
)


# ----------------------------------------------------------------------------
# PARTE A: ejemplo con RDDs
# ----------------------------------------------------------------------------
def ejemplo_rdd(df_t):
    """Repite con RDDs algunos análisis hechos con DataFrames."""

    # Un DataFrame se convierte en RDD con .rdd: cada elemento es una fila (Row).
    # map: transforma cada fila en un par (país, ingreso)
    pares = (
        df_t.select("Country", "Revenue").rdd
        .map(lambda fila: (fila["Country"], fila["Revenue"]))
    )
    # reduceByKey: suma los ingresos de cada país. Combina los valores dentro de
    # cada partición antes de moverlos, por eso es más eficiente que groupByKey.
    ingresos_pais = pares.reduceByKey(add)

    print("\n=== RDD 1: INGRESOS POR PAÍS (map + reduceByKey) ===")
    print(f"Países (count):          {ingresos_pais.count()}")
    print("Top 5 (sortBy + take):")
    for pais, valor in ingresos_pais.sortBy(lambda kv: -kv[1]).take(5):
        print(f"  {pais:<16}{valor:>14,.2f}")
    total = ingresos_pais.map(lambda kv: kv[1]).reduce(add)
    print(f"Ingreso total (reduce):  {total:,.2f}")

    # filter: se queda solo con los elementos que cumplen la condición
    sin_uk = ingresos_pais.filter(lambda kv: kv[0] != "United Kingdom")
    print("\n=== RDD 2: SIN REINO UNIDO (filter) ===")
    print(f"Países (count):          {sin_uk.count()}")
    print(f"Ingreso total (reduce):  {sin_uk.map(lambda kv: kv[1]).reduce(add):,.2f}")

    # flatMap: cada descripción produce VARIAS palabras (map produciría una sola
    # salida por fila). Es el clásico conteo de palabras (word count).
    palabras = (
        df_t.select("Description").rdd
        .flatMap(lambda fila: fila["Description"].upper().split())
        .filter(lambda p: len(p) > 3)          # ignora palabras cortas (de, of, set...)
        .map(lambda p: (p, 1))
        .reduceByKey(add)
    )
    print("\n=== RDD 3: PALABRAS MÁS FRECUENTES EN NOMBRES DE PRODUCTO (flatMap) ===")
    for palabra, veces in palabras.sortBy(lambda kv: -kv[1]).take(10):
        print(f"  {palabra:<12}{veces:>8,}")


# ----------------------------------------------------------------------------
# PARTE B: gráficas
# ----------------------------------------------------------------------------
def dibujar_tendencia(meses, ingresos, ruta):
    """Línea de ingresos mensuales (en miles de libras)."""
    miles = [v / 1000 for v in ingresos]
    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(meses[:-1], miles[:-1], marker="o", color="#1f77b4")
    # El último mes (dic. 2011) es parcial: llega solo hasta el 9 de diciembre
    ax.plot(meses[-2:], miles[-2:], linestyle="--", color="#1f77b4")
    ax.plot(meses[-1], miles[-1], marker="o", markerfacecolor="white", color="#1f77b4")
    ax.annotate("dic. 2011:\nmes parcial\n(hasta el 9)", (meses[-1], miles[-1]),
                textcoords="offset points", xytext=(-70, 25), fontsize=8)
    ax.set_title("Ingresos mensuales (Online Retail II, 2009-2011)")
    ax.set_xlabel("Mes")
    ax.set_ylabel("Ingresos (miles de £)")
    ax.grid(alpha=0.3)
    plt.xticks(rotation=60, ha="right")
    fig.tight_layout()
    fig.savefig(ruta, dpi=120)
    plt.close(fig)


def dibujar_barras_h(etiquetas, valores, titulo, xlabel, ruta, color="#2ca02c"):
    """Barras horizontales ordenadas de mayor a menor (la mayor arriba)."""
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(etiquetas, valores, color=color)
    ax.invert_yaxis()                      # el primero de la lista queda arriba
    ax.set_title(titulo)
    ax.set_xlabel(xlabel)
    ax.grid(axis="x", alpha=0.3)
    for i, v in enumerate(valores):
        ax.text(v, i, f" {v:,.0f}", va="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(ruta, dpi=120)
    plt.close(fig)


def graficar(df_t, devol, carpeta="graficas"):
    """Obtiene los resultados agregados de Spark (pocas filas) y los grafica."""
    os.makedirs(carpeta, exist_ok=True)

    # Gráfica 1: tendencia mensual (25 filas; collect() es seguro aquí)
    filas = tendencia_mensual(df_t).collect()
    dibujar_tendencia(
        [f["YearMonth"] for f in filas],
        [float(f["Ingresos"]) for f in filas],
        os.path.join(carpeta, "1_ingresos_mensuales.png"),
    )

    # Gráfica 2: top 10 productos por ingresos NETOS (ventas - devoluciones)
    filas = top_productos_netos(df_t, devol, 10).collect()
    dibujar_barras_h(
        [f"{f['StockCode']} - {f['Descripcion'][:28]}" for f in filas],
        [float(f["IngresosNetos"]) for f in filas],
        "Top 10 productos por ingresos netos",
        "Ingresos netos (£)",
        os.path.join(carpeta, "2_top_productos_netos.png"),
    )

    # Gráfica 3: top 10 países sin Reino Unido
    filas = ingresos_por_pais(df_t, excluir_uk=True).limit(10).collect()
    dibujar_barras_h(
        [f["Country"] for f in filas],
        [float(f["Ingresos"]) for f in filas],
        "Top 10 países por ingresos (sin Reino Unido)",
        "Ingresos (£)",
        os.path.join(carpeta, "3_top_paises_sin_uk.png"),
        color="#ff7f0e",
    )
    print(f"\nGráficas guardadas en la carpeta '{carpeta}/'")


# ----------------------------------------------------------------------------
def main():
    spark = crear_spark()
    df = cargar_datos(spark)
    df_ventas, df_cancelaciones = limpiar_datos(df)
    df_t = transformar_datos(df_ventas)
    devol = preparar_devoluciones(df_cancelaciones)

    ejemplo_rdd(df_t)
    graficar(df_t, devol)

    spark.stop()


if __name__ == "__main__":
    main()
