"""
convertir_xlsx_a_csv.py

Convierte el archivo original de UCI "online_retail_II.xlsx" (dataset Online Retail II)
en dos archivos CSV, uno por hoja, listos para cargarse en HDFS y leerse con Spark.

Fuente original:
    UCI Machine Learning Repository - Online Retail II (CC BY 4.0)
    https://archive.ics.uci.edu/dataset/502/online+retail+ii

Por qué se convierte:
    Spark lee CSV de forma nativa, pero no lee .xlsx sin librerías adicionales.

Qué hace (y qué NO hace):
    - NO limpia ni modifica los valores: la limpieza se hace después, en Spark.
    - Mantiene los nombres de columna originales del archivo.
    - Guarda InvoiceDate en formato estándar "yyyy-MM-dd HH:mm:ss", para evitar
      problemas con formatos regionales (dd/mm/aaaa vs mm/dd/aaaa).
    - Guarda "Customer ID" como número entero (sin ".0") y deja vacío si no existe.
    - Guarda Invoice y StockCode como texto, porque mezclan números y letras
      (por ejemplo facturas "C489449" o códigos "85123A", "POST").

Uso:
    pip install pandas openpyxl
    python3 convertir_xlsx_a_csv.py online_retail_II.xlsx ./salida

Salida:
    ./salida/online_retail_2009_2010.csv
    ./salida/online_retail_2010_2011.csv
"""

import os
import sys

import pandas as pd

# Nombre de cada hoja del Excel -> nombre del CSV de salida
HOJAS = {
    "Year 2009-2010": "online_retail_2009_2010.csv",
    "Year 2010-2011": "online_retail_2010_2011.csv",
}


def convertir(ruta_xlsx: str, carpeta_salida: str) -> None:
    """Lee cada hoja del Excel y la guarda como CSV (UTF-8) en carpeta_salida."""
    os.makedirs(carpeta_salida, exist_ok=True)

    for hoja, nombre_csv in HOJAS.items():
        print(f"Leyendo hoja '{hoja}' ...")
        # dtype=str en Invoice y StockCode evita que pandas mezcle números y texto
        df = pd.read_excel(
            ruta_xlsx,
            sheet_name=hoja,
            engine="openpyxl",
            dtype={"Invoice": str, "StockCode": str},
        )

        # Customer ID viene como decimal (13085.0) porque tiene vacíos;
        # Int64 permite enteros con valores nulos.
        df["Customer ID"] = df["Customer ID"].astype("Int64")

        ruta_csv = os.path.join(carpeta_salida, nombre_csv)
        df.to_csv(
            ruta_csv,
            index=False,
            encoding="utf-8",
            date_format="%Y-%m-%d %H:%M:%S",  # formato de fecha estándar
        )
        print(f"  -> {ruta_csv}: {len(df):,} filas, {len(df.columns)} columnas")

    print("Conversión terminada.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python3 convertir_xlsx_a_csv.py <archivo.xlsx> <carpeta_salida>")
        sys.exit(1)
    convertir(sys.argv[1], sys.argv[2])
