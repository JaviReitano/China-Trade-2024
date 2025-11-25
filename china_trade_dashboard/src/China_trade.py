import pandas as pd

df = pd.read_csv(
    r"C:\Users\Usuario\Desktop\Curso Titularizado Python\china_trade_dashboard\data\processed\2025011810224811354.csv",
    sep=';',
    header=None,
    engine='python',
    on_bad_lines='skip'
)

'print(df.head())'

# Filtracion solo filas donde la columna 1 tiene formato AAAA.MM (2023.01, 2024.02, etc.)
df_monthly = df[df[1].astype(str).str.match(r"^\d{4}\.\d{2}$", na=False)]
"""
print("Filas con meses detectadas:")
print(df_monthly.head(15))
print(df_monthly.tail(5))"""

# Nos quedamos con las columnas útiles y les damos nombres claros
df_clean = df_monthly[[1, 2, 3, 4, 5, 6, 7, 8, 9]].copy()

df_clean.columns = [
    "year_month",     # 2023.01, 2023.02, etc.
    "total_month",    # total del mes
    "exports_month",  # exportaciones del mes
    "imports_month",  # importaciones del mes
    "balance_month",  # saldo del mes
    "total_cum",      # total acumulado en el año
    "exports_cum",    # exportaciones acumuladas
    "imports_cum",    # importaciones acumuladas
    "balance_cum"     # saldo acumulado
]
"""
print("\nDataFrame limpio con nombres de columnas:")
print(df_clean.head(15))"""

# Convertir 'year_month' a columnas numéricas year, month
df_clean["year"] = df_clean["year_month"].str.slice(0, 4).astype(int)
df_clean["month"] = df_clean["year_month"].str.slice(5, 7).astype(int)

#print("\nCon columnas year y month:")
#print(df_clean[["year_month", "year", "month"]].head(15))

# Convertir columnas numéricas
numeric_cols = [
    "total_month", "exports_month", "imports_month", "balance_month",
    "total_cum", "exports_cum", "imports_cum", "balance_cum"
]

for col in numeric_cols:
    df_clean[col] = (
        df_clean[col]
        .astype(str)
        .str.replace(" ", "")   # eliminar espacios
        .str.replace(",", "")   # por si hay comas
        .str.replace("\u3000", "")  # espacio chino
        .str.replace("\xa0", "")    # espacio no-break
    )
    df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")

#print("\nTipos de datos después de convertir:")
#print(df_clean.dtypes)

df_clean["date"] = pd.to_datetime(df_clean["year"].astype(str) + "-" + df_clean["month"].astype(str) + "-01")
#print(df_clean[["year_month", "date"]].head(12))

#print(df_clean.dtypes)
#print(df_clean.head())

from pathlib import Path

# Ruta de salida (podés adaptarla si querés)
output_path_csv = Path(r"C:\Users\Usuario\Desktop\Curso Titularizado Python\china_trade_dashboard\data\processed\gac_monthly_clean.csv")
output_path_parquet = Path(r"C:\Users\Usuario\Desktop\Curso Titularizado Python\china_trade_dashboard\data\processed\gac_monthly_clean.parquet")

# Crear carpeta
output_path_csv.parent.mkdir(parents=True, exist_ok=True)

# Guardar CSV (para Power BI, Excel, etc.)
df_clean.to_csv(output_path_csv, index=False, encoding="utf-8-sig")

# Guardar Parquet (para futuros proyectos en Python)
df_clean.to_parquet(output_path_parquet, index=False)

#print("\nArchivos guardados:")
#print(f"- CSV: {output_path_csv}")
#print(f"- Parquet: {output_path_parquet}")


#---------------------ARCHIVO DE REGIONES--------------------------------------------------

# 1) Ruta del archivo de ENERO por región
path_jan = r"C:\Users\Usuario\Desktop\Curso Titularizado Python\china_trade_dashboard\data\raw\region\region_2024_01.csv"

df_jan_raw = pd.read_csv(
    path_jan,
    sep=";",          # vienen con punto y coma
    header=None,      # sin encabezados
    engine="python",
    encoding="utf-8",
    on_bad_lines="skip"
)

print("Primeras filas de enero (raw):")
print(df_jan_raw.head(20))
print("\nShape enero:", df_jan_raw.shape)

# Filtramos filas de columna 1 que parecen tener datos de región:
# - columna 1 no vacía (nombre región)
# - columna 2 con números (valores)
df_jan_regions = df_jan_raw.copy()

# Convertimos col 2 a string y nos quedamos con las filas que tienen dígitos
mask_numeric = df_jan_regions[2].astype(str).str.contains(r"\d", na=False)
mask_region  = df_jan_regions[1].notna()

df_jan_regions = df_jan_regions[mask_numeric & mask_region]

print("\nFilas de enero consideradas como 'regiones':")
print(df_jan_regions.head(15))
print("\nTotal filas regiones enero:", df_jan_regions.shape[0])

# Seleccionamos columnas 1 a 10 (nombre región + números)
df_jan_clean = df_jan_regions[[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]].copy()

df_jan_clean.columns = [
    "region_raw",     # nombre de la región (en chino)
    "v1", "v2", "v3", "v4", "v5", "v6",  # valores (miles, acumulados, etc.)
    "pct1", "pct2", "pct3"               # porcentajes (YoY, etc.)
]

#print("\nEstructura enero limpia (solo columnas útiles):")
#print(df_jan_clean.head(10))

df_jan_clean["year"] = 2024
df_jan_clean["month"] = 1
df_jan_clean["date"] = pd.to_datetime(
    df_jan_clean["year"].astype(str) + "-" +
    df_jan_clean["month"].astype(str) +
    "-01"
)

print("\nEnero con columnas de fecha:")
print(df_jan_clean[["region_raw", "year", "month", "date"]].head(10))

numeric_cols_region = ["v1", "v2", "v3", "v4", "v5", "v6", "pct1", "pct2", "pct3"]

for col in numeric_cols_region:
    df_jan_clean[col] = (
        df_jan_clean[col]
        .astype(str)
        .str.replace(" ", "")
        .str.replace(".", "")   # quitamos puntos
        .str.replace(",", "")   # por si apareciera coma
    )
    df_jan_clean[col] = pd.to_numeric(df_jan_clean[col], errors="coerce")

print("\nTipos de datos de enero regiones:")
print(df_jan_clean.dtypes)

# Renombrar columnas 
df_jan_clean = df_jan_clean.rename(columns={
    "v1": "trade_total_month",      # import + export del mes
    "v2": "trade_total_cum",        # import + export acumulado
    "v3": "exports_month",          # exportaciones del mes
    "v4": "exports_cum",            # exportaciones acumuladas
    "v5": "imports_month",          # importaciones del mes
    "v6": "imports_cum",            # importaciones acumuladas
    "pct1": "trade_total_cum_yoy",  # % YoY del total acumulado
    "pct2": "exports_cum_yoy",      # % YoY export acumulada
    "pct3": "imports_cum_yoy",      # % YoY import acumulada
})

print("\nEnero con nombres finales de columnas:")
print(df_jan_clean.head(10))

