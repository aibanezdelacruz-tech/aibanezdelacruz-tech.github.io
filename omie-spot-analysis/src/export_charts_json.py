"""
export_charts_json.py
---------------------
Genera los archivos JSON con datos para las visualizaciones de GitHub Pages.

Ejecutar después de haber procesado los datos con load_data.py.
Los JSON se guardan en docs/ para ser servidos por GitHub Pages.
"""

import json
from pathlib import Path
import pandas as pd
import numpy as np

DATA_PATH = Path(__file__).parent.parent / "data" / "processed" / "omie_precios.parquet"
DOCS_PATH = Path(__file__).parent.parent / "docs"


def export_heatmap_data(df: pd.DataFrame) -> None:
    """
    Exporta la matriz hora×día-semana para el heatmap de GitHub Pages.
    Formato: { hour: [...], dayofweek: [...], price: [...] }
    """
    # Filtrar año más reciente para el heatmap de la web
    año_reciente = df.index.year.max()
    df_recent = df[df.index.year == año_reciente].copy()
    df_recent["hour"] = df_recent.index.hour
    df_recent["dow"]  = df_recent.index.dayofweek

    pivot = df_recent.groupby(["hour", "dow"])["precio_esp"].mean().unstack()
    pivot = pivot.round(2)

    payload = {
        "year": int(año_reciente),
        "hours": list(range(24)),
        "days":  ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"],
        "matrix": [list(pivot.iloc[h]) for h in range(24)],
    }
    out = DOCS_PATH / "heatmap_data.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"[OK] heatmap_data.json -> {out}")


def export_timeseries_data(df: pd.DataFrame) -> None:
    """
    Exporta la media diaria de precios para el gráfico de evolución.
    Últimos 2 años para mantener el JSON manejable.
    """
    df_daily = df["precio_esp"].resample("D").mean().round(2)
    # Solo los 2 años más recientes
    df_daily = df_daily[df_daily.index >= df_daily.index.max() - pd.DateOffset(years=2)]

    payload = {
        "dates":  [str(d.date()) for d in df_daily.index],
        "prices": [None if pd.isna(v) else round(v, 2) for v in df_daily.values],
    }
    out = DOCS_PATH / "timeseries_data.json"
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"[OK] timeseries_data.json -> {out}")


def export_stats_summary(df: pd.DataFrame) -> None:
    """Exporta estadísticas clave para las tarjetas de la landing page."""
    stats = {
        "periodo": f"{df.index.min().date()} / {df.index.max().date()}",
        "precio_medio": round(float(df["precio_esp"].mean()), 2),
        "precio_max": round(float(df["precio_esp"].max()), 2),
        "precio_min": round(float(df["precio_esp"].min()), 2),
        "horas_negativas": int((df["precio_esp"] < 0).sum()),
        "horas_total": int(df["precio_esp"].notna().sum()),
        "volatilidad": round(float(df["precio_esp"].std()), 2),
    }
    out = DOCS_PATH / "stats.json"
    out.write_text(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"[OK] stats.json -> {out}")


if __name__ == "__main__":
    if not DATA_PATH.exists():
        print("[ERROR] Primero ejecuta src/load_data.py para generar el Parquet procesado.")
        exit(1)

    df = pd.read_parquet(DATA_PATH)
    DOCS_PATH.mkdir(exist_ok=True)
    export_heatmap_data(df)
    export_timeseries_data(df)
    export_stats_summary(df)
    print("\n[OK] JSONs listos. Copia docs/ al repo para GitHub Pages.")
