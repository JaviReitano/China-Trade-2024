import pandas as pd
from pathlib import Path

# === RUTAS BASE ===
BASE_DIR = Path(__file__).resolve().parent.parent  # raíz del repo
PROCESSED_DIR = BASE_DIR / "data" / "processed"
EXTERNAL_DIR = BASE_DIR / "data" / "external"

# Archivos de entrada
REGIONS_PARQUET = PROCESSED_DIR / "gac_regions_2024.parquet"
REGIONS_CSV = PROCESSED_DIR / "gac_regions_2024.csv"  # por si tenés también CSV
TRANSLATIONS_XLSX = EXTERNAL_DIR / "regions_translated.xlsx"

# Archivos de salida
OUTPUT_CSV = PROCESSED_DIR / "gac_regions_2024_with_names.csv"
OUTPUT_PARQUET = PROCESSED_DIR / "gac_regions_2024_with_names.parquet"


def load_regions() -> pd.DataFrame:
    """Carga el dataset de regiones 2024 ya limpio (el que generamos ayer)."""
    if REGIONS_PARQUET.exists():
        df = pd.read_parquet(REGIONS_PARQUET)
        print(f"[OK] Cargado dataset de regiones desde {REGIONS_PARQUET}")
    elif REGIONS_CSV.exists():
        df = pd.read_csv(REGIONS_CSV)
        print(f"[OK] Cargado dataset de regiones desde {REGIONS_CSV}")
    else:
        raise FileNotFoundError(
            f"No encontré ni {REGIONS_PARQUET} ni {REGIONS_CSV}. "
            "Asegurate de haber corrido antes el script de limpieza de regiones."
        )

    print("Columnas en df_regiones:", df.columns.tolist())
    return df


def load_translations() -> pd.DataFrame:
    """Carga el diccionario de traducciones chino → inglés."""
    if not TRANSLATIONS_XLSX.exists():
        raise FileNotFoundError(
            f"No encontré el archivo de traducciones: {TRANSLATIONS_XLSX}"
        )

    df_tr = pd.read_excel(TRANSLATIONS_XLSX)

    # Normalizamos nombres de columnas: habiendo una columna en chino y otra en ingles
    # Ajustá estos nombres si tus encabezados son distintos.
    if df_tr.shape[1] < 2:
        raise ValueError(
            "El archivo de traducciones debe tener al menos dos columnas "
            "(chino e inglés)."
        )

    # Tomamos las dos primeras columnas
    col_ch = df_tr.columns[0]
    col_en = df_tr.columns[1]

    df_tr = df_tr[[col_ch, col_en]].copy()
    df_tr.columns = ["region_raw", "region_name_en"]

    # Limpieza básica de texto
    df_tr["region_raw"] = df_tr["region_raw"].astype(str).str.strip()
    df_tr["region_name_en"] = df_tr["region_name_en"].astype(str).str.strip()

    print("Ejemplo de diccionario de traducciones:")
    print(df_tr.head())

    return df_tr


def merge_regions_and_names(df_regions: pd.DataFrame, df_tr: pd.DataFrame) -> pd.DataFrame:
    """
    Une el dataset de regiones con el diccionario de traducciones.
    """
    # También limpiamos region_raw en el dataset de regiones por si hay espacios
    df_regions = df_regions.copy()
    df_regions["region_raw"] = df_regions["region_raw"].astype(str).str.strip()

    df_merged = df_regions.merge(
        df_tr,
        how="left",
        on="region_raw",
    )

    # Chequeo: ¿hay regiones sin traducción?
    missing = df_merged[df_merged["region_name_en"].isna()]["region_raw"].unique()
    if len(missing) > 0:
        print("\nATENCIÓN: Hay regiones sin traducción en el diccionario:")
        for r in missing:
            print("  -", r)
        print(
            "\nPodés agregarlas a regions_translated.xlsx y volver a ejecutar este script."
        )
    else:
        print("\nTodas las regiones tienen traducción. ✅")

    return df_merged


def save_outputs(df: pd.DataFrame):
    """Guarda el resultado final en CSV y Parquet."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[OK] Guardado CSV final: {OUTPUT_CSV}")

    try:
        df.to_parquet(OUTPUT_PARQUET, index=False)
        print(f"[OK] Guardado Parquet final: {OUTPUT_PARQUET}")
    except Exception as e:
        print(f"[WARN] No se pudo guardar Parquet: {e}")


if __name__ == "__main__":
    # 1) Cargamos datos limpios de regiones
    df_regions = load_regions()

    # 2) Cargamos diccionario de traducciones (tu archivo)
    df_tr = load_translations()

    # 3) Merge
    df_final = merge_regions_and_names(df_regions, df_tr)

    print("\nPreview del dataset final:")
    print(df_final.head())

    # 4) Guardar
    save_outputs(df_final)

print("Primeras filas:")
print(df_all.head())


# === GUARDAR ARCHIVOS PROCESADOS ===
output_csv = OUTPUT_DIR / "gac_regions_2024.csv"
output_parquet = OUTPUT_DIR / "gac_regions_2024.parquet"

df_all.to_csv(output_csv, index=False)
print(f"CSV guardado en: {output_csv}")

try:
    df_all.to_parquet(output_parquet, index=False)
    print(f"Parquet guardado en: {output_parquet}")
except Exception as e:
    print(f"No se pudo guardar Parquet: {e}")

from pathlib import Path

# === GUARDAR ARCHIVOS PROCESADOS ===
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

output_csv = OUTPUT_DIR / "gac_regions_2024.csv"

df_all.to_csv(output_csv, index=False)
print(f"CSV guardado en: {output_csv}")