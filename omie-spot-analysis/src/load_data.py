"""
load_data.py
------------
Carga y procesa los archivos diarios de precios OMIE (marginalpdbc).

Formato de entrada: año;mes;día;hora;precio_ESP;precio_POR (separador ';')
Salida: DataFrame con DatetimeIndex y columnas precio_esp, precio_por
"""

import pandas as pd
import numpy as np
from pathlib import Path


# ── Rutas por defecto ─────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
OUT_PATH  = Path(__file__).parent.parent / "data" / "processed" / "omie_precios.parquet"


def _parse_single_file(filepath: Path) -> pd.DataFrame | None:
    """Lee un único fichero TXT de OMIE y devuelve un DataFrame limpio."""
    try:
        df = pd.read_csv(
            filepath,
            sep=";",
            header=None,
            skiprows=1,    # primera línea es cabecera 'MARGINALPDBC;'
            names=["year", "month", "day", "hour", "precio_esp", "precio_por", "extra"],
            engine="python",
        )
        # Eliminar filas de separador ('*')
        df = df[pd.to_numeric(df["year"], errors="coerce").notna()].copy()
        df = df.astype({"year": int, "month": int, "day": int, "hour": int,
                        "precio_esp": float, "precio_por": float})

        # Hora 25 existe en cambio de hora de otoño → la mapeamos a hora 3
        df["hour"] = df["hour"].clip(upper=24)

        # Construir datetime: hora OMIE es 1-indexed; usamos hora-1
        df["datetime"] = pd.to_datetime(
            df["year"].astype(str)
            + "-" + df["month"].astype(str).str.zfill(2)
            + "-" + df["day"].astype(str).str.zfill(2)
            + " " + (df["hour"] - 1).astype(str).str.zfill(2) + ":00",
            format="%Y-%m-%d %H:%M",
        )

        return df[["datetime", "precio_esp", "precio_por"]].reset_index(drop=True)

    except Exception as e:
        print(f"  [WARN] Error en {filepath.name}: {e}")
        return None


def load_omie_dataset(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """
    Carga todos los ficheros OMIE en data_dir y devuelve un único DataFrame
    ordenado por datetime.

    Parámetros
    ----------
    data_dir : Path  Carpeta con los .txt de OMIE (un fichero por día).

    Retorna
    -------
    pd.DataFrame con columnas: datetime (index), precio_esp, precio_por
    """
    txt_files = sorted(data_dir.glob("*.txt"))
    if not txt_files:
        raise FileNotFoundError(
            f"No se encontraron archivos .txt en '{data_dir}'. "
            "Copia los ficheros OMIE en data/raw/ "
            "(o ajusta DATA_DIR en load_data.py)."
        )

    print(f"[INFO] Cargando {len(txt_files)} ficheros OMIE desde '{data_dir.name}'...")
    frames = [_parse_single_file(f) for f in txt_files]
    frames = [f for f in frames if f is not None and len(f) > 0]

    df = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates("datetime")
        .sort_values("datetime")
        .set_index("datetime")
    )

    # Reemplazar ceros por NaN (OMIE usa 0.0 en festivos/datos faltantes)
    df.replace(0.0, np.nan, inplace=True)

    print(f"[OK] Dataset cargado: {len(df):,} registros")
    print(f"   Periodo : {df.index.min().date()} - {df.index.max().date()}")
    print(f"   Nulos   : {df['precio_esp'].isna().sum():,} horas ({df['precio_esp'].isna().mean():.1%})")
    print(f"   Precio medio ESP: {df['precio_esp'].mean():.2f} EUR/MWh")
    return df


def save_processed(df: pd.DataFrame, out_path: Path = OUT_PATH) -> None:
    """Guarda el dataset procesado en formato Parquet."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path)
    print(f"[OK] Guardado en '{out_path}'")


if __name__ == "__main__":
    df = load_omie_dataset()
    save_processed(df)
    print("\nPrimeras filas:")
    print(df.head(10))
