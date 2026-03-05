"""
features.py
-----------
Feature engineering para series temporales de precios OMIE.

Genera variables explicativas relevantes para el mercado eléctrico:
- Temporales: hora, día de semana, mes, estación, festivo
- Lags: precio hace 1h, 24h (mismo día anterior) y 168h (misma hora semana anterior)
- Estadísticas móviles: media y volatilidad de las últimas 24h
- Exógenas ESIOS: mix de generación real (Solar, Eólico, Nuclear, Gas, Hidro) + demanda
"""

import pandas as pd
import numpy as np
from pathlib import Path


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Festivos nacionales España (días donde el mercado tiene patrones distintos)
FESTIVOS_ES = [
    "01-01", "01-06", "05-01", "08-15",
    "10-12", "11-01", "12-06", "12-08", "12-25",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_holiday_es(dt_index: pd.DatetimeIndex) -> np.ndarray:
    """Devuelve array bool: True si el día es festivo nacional español."""
    mmdd = dt_index.strftime("%m-%d")
    return np.isin(mmdd, FESTIVOS_ES)


def _resolve_project_root(cwd: Path) -> Path:
    """Resuelve la raíz del repositorio tanto desde notebooks/ como desde raíz."""
    if cwd.name == "notebooks":
        return cwd.parent
    return cwd


# ---------------------------------------------------------------------------
# Ingeniería de features temporales
# ---------------------------------------------------------------------------

def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade variables de calendario al DataFrame.

    Variables creadas:
    - hour         : hora del día (0-23)
    - dayofweek    : día de semana (0=lunes, 6=domingo)
    - month        : mes (1-12)
    - season       : estación (0=invierno, 1=primavera, 2=verano, 3=otoño)
    - is_weekend   : 1 si sábado/domingo
    - is_holiday   : 1 si festivo nacional español
    """
    df = df.copy()
    idx = df.index

    df["hour"]       = idx.hour
    df["dayofweek"]  = idx.dayofweek
    df["month"]      = idx.month
    df["is_weekend"] = (idx.dayofweek >= 5).astype(int)
    df["is_holiday"] = is_holiday_es(idx).astype(int)

    # Estación: invierno→0, primavera→1, verano→2, otoño→3
    df["season"] = pd.cut(idx.month,
                          bins=[0, 2, 5, 8, 11, 12],
                          labels=[0, 1, 2, 3, 0],
                          ordered=False).astype(int)
    return df


def add_lag_features(df: pd.DataFrame, target: str = "precio_esp") -> pd.DataFrame:
    """
    Añade lags del precio spot, clave en forecasting energético:

    - lag_1h   : precio 1 hora antes   → captura autocorrelación a corto plazo
    - lag_24h  : precio mismo día D-1  → efecto diario (Baseline de persistencia)
    - lag_168h : precio misma hora S-1 → efecto semanal (laborable/festivo)
    """
    df = df.copy()
    df["lag_1h"]   = df[target].shift(1)
    df["lag_24h"]  = df[target].shift(24)
    df["lag_168h"] = df[target].shift(168)   # 24h × 7 días
    return df


def add_rolling_features(df: pd.DataFrame, target: str = "precio_esp") -> pd.DataFrame:
    """
    Añade estadísticas de ventana deslizante:

    - ma_24h  : media últimas 24h  → nivel reciente del mercado
    - std_24h : volatilidad 24h    → tensión/estabilidad del mercado
    - ma_168h : media última semana → tendencia semanal
    """
    df = df.copy()
    df["ma_24h"]  = df[target].shift(1).rolling(24,  min_periods=12).mean()
    df["std_24h"] = df[target].shift(1).rolling(24,  min_periods=12).std()
    df["ma_168h"] = df[target].shift(1).rolling(168, min_periods=84).mean()
    return df


# ---------------------------------------------------------------------------
# Ingeniería de features exógenas ESIOS
# ---------------------------------------------------------------------------

def add_esios_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Integra las características exógenas de ESIOS (Mix de Generación + Demanda).
    Requiere que el notebook 01_data_extraction.ipynb haya exportado esios_features.parquet
    o que el mix_generacion esté disponible.

    Variables exógenas añadidas:
    - eolica, solar_fv, nuclear, hidraulica, gas_ccgt  (MWh generados - generación T.Real nacional)
    - renovable_mw: suma de generación renovable (eólica + solar + hidráulica) [MWh]
    - total_mw: generación nacional total [MWh]
    - demanda_prev: demanda prevista nacional [MWh]

    IDs oficiales de ESIOS/REE - Generación T.Real Nacional (2025-03-05):
    - 2038: Eólica T.Real nacional [MWh]
    - 2044: Solar FV T.Real nacional [MWh]
    - 2039: Nuclear T.Real nacional [MWh]
    - 2042: Hidráulica T.Real nacional [MWh]
    - 2041: Ciclo Combinado T.Real nacional [MWh]
    - 2037: Demanda real nacional [MWh]
    NOTA: IDs anteriores (73, 74, 79, 10008, 10010, 10063) fueron descartados durante auditoría de datos.

    Dataset ESIOS validado:
    - 24,096 horas (2023-01-01 → 2025-09-30)
    - Suma de componentes = Total nacional: diferencia 0% ✅
    - Sin normalización: valores directos de ESIOS [MWh]
    """
    df = df.copy()

    root = _resolve_project_root(Path.cwd())
    esios_path = root / 'data' / 'processed' / 'esios_features.parquet'

    if not esios_path.exists():
        print(f"  [WARN] No se encuentra {esios_path.name}. Omitiendo ESIOS.")
        print("         Ejecuta 01_data_extraction.ipynb para generar la cache.")
        return df

    df_esios = pd.read_parquet(esios_path)

    # --- Imputación selectiva para indicadores con gaps muy limitados ---
    # Únicamente se interpola gas_ccgt si tiene algún gap residual (<0.1%)
    for col in ['gas_ccgt']:
        if col in df_esios.columns:
            n_nan = df_esios[col].isna().sum()
            if n_nan > 0:
                pct_nan = (n_nan / len(df_esios) * 100)
                if pct_nan < 1.0:  # Solo si hay muy pocos gaps
                    df_esios[col] = (df_esios[col]
                                     .interpolate(limit=168)   # max 1 semana
                                     .bfill(limit=24 * 7))
                    print(f"  [ESIOS] Imputados {n_nan:,} NaN en '{col}' ({pct_nan:.2f}%)")

    # Merge left: preserva todas las horas OMIE, añade ESIOS donde esté disponible
    df = df.join(df_esios, how='left')
    return df


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def build_feature_matrix(df: pd.DataFrame,
                          target: str = "precio_esp",
                          dropna: bool = True,
                          use_esios: bool = True) -> pd.DataFrame:
    """
    Pipeline completo: a partir del DataFrame OMIE con DatetimeIndex,
    devuelve la matriz de features + target lista para modelar.

    Parametros
    ----------
    df         : DataFrame con columna 'precio_esp' (y opcionalmente 'precio_por')
    target     : nombre de la variable objetivo
    dropna     : si True, elimina filas con NaN (necesario para los lags D-7)
    use_esios  : si True, incorpora las features de mix de generacion de ESIOS

    Retorna
    -------
    DataFrame con todas las features + columna target
    """
    df = add_calendar_features(df)
    df = add_lag_features(df, target)
    df = add_rolling_features(df, target)

    if use_esios:
        df = add_esios_features(df)

    # Spread MIBEL: diferencia de precios España-Portugal del dia anterior.
    # Indicador de congestion en las interconexiones y coyuntura del mercado iberico.
    if "precio_por" in df.columns:
        df["spread_mibel"] = (df["precio_esp"] - df["precio_por"]).shift(24)
    else:
        df["spread_mibel"] = 0.0   # Fallback si no hay datos de Portugal

    if dropna:
        n_before = len(df)
        df = df.dropna()
        n_removed = n_before - len(df)
        print(f"  Features generadas. NaN eliminados: {n_removed} filas de warmup/gaps.")

    print(f"   Shape final: {df.shape}  --  {len(df):,} horas disponibles para modelo")
    return df


# ---------------------------------------------------------------------------
# Columnas de features para el modelo
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    # Temporales
    "hour", "dayofweek", "month", "season",
    "is_weekend", "is_holiday",
    # Autoregresivas (Lag)
    "lag_1h", "lag_24h", "lag_168h",
    # Estadisticas de ventana
    "ma_24h", "std_24h", "ma_168h",
    # Mercado MIBEL
    "spread_mibel",
]

# Features exógenas físicas de Red Eléctrica (ESIOS/REE)
# Capturan el efecto del merit order sobre el precio marginalista:
#   - Renovables (solar, eolica) → precio depresor (coste marginal ~ 0)
#   - Nuclear                   → baseload, precio suelo ~0
#   - Gas (CCGT)                → precio techo (precio gas TTF + CO2)
#   - Hidraulica                → flexible, efecto regulador
# Fuente: API ESIOS/REE - IDs oficiales Generación T.Real Nacional (2025-03-05)
# IDs correctos: 2038 (Eólica), 2044 (Solar), 2039 (Nuclear), 2042 (Hidráulica), 2041 (Gas CCGT)
ESIOS_FEATURE_COLS = [
    "eolica", "solar_fv",           # Renovables variables (0 coste marginal)
    "nuclear", "hidraulica",         # Baseload flexible
    "gas_ccgt",                      # Tecnología marginal
    "renovable_mw",                  # Suma de renovables (MWh)
    "total_mw",                      # Generación nacional total (MWh)
]
