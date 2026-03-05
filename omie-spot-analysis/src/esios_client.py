"""
esios_client.py
---------------
Wrapper sobre `python-esios` (datons/python-esios) para el proyecto omie-spot-analysis.

Referencia metodologica:
  https://github.com/datons/python-esios   (libreria oficial — GPL-3.0)

===========================================================================
POLITICA DE CACHE (obligatoria por terminos de uso REE/ESIOS):
  - REE requiere NO realizar peticiones redundantes ni masivas.
  - REE requiere que las webs/apps sirvan datos desde servidor propio,
    NO directamente de su API.
  - Cache local en Parquet: data/processed/esios/<id>_<start>_<end>.parquet
  - Si el archivo existe, se usa directamente SIN llamar a la API.
  - Solo se llama a la API si el rango solicitado no esta en cache.
===========================================================================

FASE 1 (sin token):  El modulo se importa sin errores y las funciones
                     devuelven DataFrames vacios con un aviso claro.
FASE 2 (con token):  Basta con crear el archivo .env con tu token ESIOS
                     (ver instrucciones de .env.example) para activar
                     todas las descargas.

=== INDICADORES CLAVE (IDs OFICIALES ESIOS T.Real Nacional) ===
  
Generación Real T.Real Nacional (2023-2025):
  600   Precio del mercado diario España [EUR/MWh]
  2037  Demanda real nacional T.Real [MWh]
  2038  Eólica T.Real nacional [MWh] ← UTILIZADO en este proyecto
  2039  Nuclear T.Real nacional [MWh] ← UTILIZADO
  2041  Ciclo combinado T.Real nacional [MWh] ← UTILIZADO
  2042  Hidráulica T.Real nacional [MWh] ← UTILIZADO
  2044  Solar fotovoltaica T.Real nacional [MWh] ← UTILIZADO

Otros indicadores (disponibles pero no usados actualmente):
  2040  Carbón T.Real nacional [MWh]
  2045  Solar térmica T.Real nacional [MWh]
  2046  Térmica renovable T.Real nacional [MWh]
  1293  Demanda prevista D+1 [MW]
  1370  Nivel de embalses peninsulares [GWh]
  1739  Intercambio neto fronteras [MW]

Referencia completa: https://api.esios.ree.es/indicators
NOTA: IDs anteriores (73, 74, 79, 10008, 10010, 10063) fueron descartados por inconsistencia de datos.
"""

from __future__ import annotations

import os
import pandas as pd
from pathlib import Path

# --- Carga del token desde .env (si existe) ----------------------------------
_dot_env = Path(__file__).parent.parent / ".env"
if _dot_env.exists():
    # utf-8-sig maneja BOM de Windows; strip de comillas cubre formatos variados
    for _line in _dot_env.read_text(encoding="utf-8-sig").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            _v = _v.strip().strip('"').strip("'")
            os.environ.setdefault(_k.strip(), _v)

# --- Estado del token --------------------------------------------------------
_TOKEN = os.getenv("ESIOS_API_KEY", "")
TOKEN_CONFIGURED = bool(_TOKEN and _TOKEN not in ("", "PEGA_AQUI_TU_TOKEN_ESIOS"))

# --- Directorio de cache local (politica REE: no peticiones redundantes) -----
_CACHE_DIR = Path(__file__).parent.parent / "data" / "processed" / "esios"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(indicator_id: int, start_date: str, end_date: str,
                time_trunc: str = "hour") -> Path:
    """Devuelve la ruta Parquet del cache para un indicador y rango de fechas."""
    safe_start = start_date.replace("-", "")
    safe_end   = end_date.replace("-", "")
    return _CACHE_DIR / f"ind{indicator_id}_{safe_start}_{safe_end}_{time_trunc}.parquet"


def _phase_warning(fn_name: str) -> None:
    """Muestra mensaje claro cuando se ejecuta en Fase 1 (sin token)."""
    print(
        f"\n[FASE 1 - SIN TOKEN] {fn_name}() no disponible sin token ESIOS.\n"
        "  Para activar FASE 2:\n"
        "    1. Solicita token a consultasios@ree.es\n"
        "    2. Crea el archivo .env en la raiz:\n"
        "         echo ESIOS_API_KEY=tu_token > .env\n"
        "  Las visualizaciones se generan con datos OMIE existentes.\n"
    )


# --- Grupos de indicadores (IDs OFICIALES de ESIOS T.Real) ---------
# === IDs CORREGIDOS SEGUN DOCUMENTACION OFICIAL ESIOS (2025-03-05) ===
# Los IDs anteriores (73, 74, 79, 10008, 10010, 10063) eran obsoletos o errados.
# 
# GENERACION REAL T.REAL NACIONAL (MWh acumulados horarios):
# Fuente oficial: Centro de control ESIOS - Indicadores de tiempo real
#   - 2038: Eölica T.Real nacional [MWh]
#   - 2039: Nuclear T.Real nacional [MWh]
#   - 2040: Carbón T.Real nacional [MWh]
#   - 2041: Ciclo combinado T.Real nacional [MWh]
#   - 2042: Hidráulica T.Real nacional [MWh]
#   - 2044: Solar fotovoltaica T.Real nacional [MWh]
#   - 2045: Solar térmica T.Real nacional [MWh]
#   - 2046: Térmica renovable T.Real nacional [MWh]
#   - 2037: Demanda real nacional [MWh]
#
# CAMBIOS RESPECTO A ANTERIOR (2025-03-05):
#   ❌ ELIMINADOS IDs errados: 73, 74, 79, 10008, 10010, 10063, 1
#   ✅ AGREGADOS IDs correctos de ESIOS T.Real nacional: 2038-2046
#   ✅ Cambio crítico: Ahora usamos IDs NACIONALES consolidados (2037-2046)
INDICADORES_PRECIO = {
    "precio_esp":  600,    # Precio pool diario Espana [EUR/MWh]
}
INDICADORES_MIX = {
    "eolica":           2038,  # Generacion eolica T.Real nacional [MWh]
    "solar_fv":         2044,  # Generacion solar FV T.Real nacional [MWh]
    "nuclear":          2039,  # Generacion nuclear T.Real nacional [MWh]
    "hidraulica":       2042,  # Generacion hidraulica T.Real nacional [MWh]
    "gas_ccgt":         2041,  # Generacion ciclo combinado T.Real nacional [MWh]
    # Nota: 2040 (carbón), 2045 (solar térmica) y 2046 (térmica renovable) pueden no tener datos completos
}
INDICADORES_SISTEMA = {
    "demanda_real":  2037,   # Demanda real nacional T.Real [MWh] (ID nacionl oficial)
    "demanda_prev":  1293,   # Demanda prevista D+1 [MW]
    "intercambio_mw": 1739,  # Intercambios netos fronteras [MW]
}



def _get_client():
    """Crea y devuelve el ESIOSClient de datons/python-esios."""
    from esios import ESIOSClient                          # noqa: PLC0415
    return ESIOSClient(token=_TOKEN if TOKEN_CONFIGURED else None)


def get_indicator(indicator_id: int,
                  start_date:   str,
                  end_date:     str,
                  time_trunc:   str = "hour",
                  force_download: bool = False) -> pd.DataFrame:
    """
    Obtiene un indicador ESIOS con cache local obligatoria (politica REE).

    Flujo:
      1. Comprueba si existe Parquet local para este indicador/rango.
      2. Si existe, lo carga y devuelve SIN llamar a la API.
      3. Si no existe (o force_download=True), descarga de la API y guarda en cache.

    Parametros
    ----------
    indicator_id   : ID del indicador ESIOS
    start_date     : Fecha inicio 'YYYY-MM-DD'
    end_date       : Fecha fin    'YYYY-MM-DD'
    time_trunc     : Granularidad ('hour', 'day', 'month')
    force_download : Si True, ignora cache y fuerza descarga desde API
                     (reservado para cuando los datos historicos han sido corregidos)

    Ejemplo
    -------
    >>> df = get_indicator(10034, "2024-01-01", "2024-12-31")  # Eolica
    """
    if not TOKEN_CONFIGURED:
        _phase_warning(f"get_indicator({indicator_id})")
        return pd.DataFrame()

    cache = _cache_path(indicator_id, start_date, end_date, time_trunc)

    # 1. Servir desde cache si existe (cumple politica REE)
    if cache.exists() and not force_download:
        print(f"  [CACHE] Indicador {indicator_id}: datos locales -> {cache.name}")
        return pd.read_parquet(cache)

    # 2. Solo llama a la API si no hay cache
    print(f"  [API]   Indicador {indicator_id}: descargando {start_date} -> {end_date}...")
    client = _get_client()
    handle = client.indicators.get(indicator_id)
    df = handle.historical(start_date, end_date, time_trunc=time_trunc)

    if df.empty:
        print(f"  [WARN] Indicador {indicator_id}: sin datos para {start_date} -> {end_date}")
        return df

    # Normalizar indice a datetime naive
    if hasattr(df.index, "tz") and df.index.tz is not None:
        df.index = df.index.tz_localize(None)

    # 3. Persistir en cache local (REE: la web debe servir datos propios, no la API)
    df.to_parquet(cache)
    print(f"  [OK]   Indicador {indicator_id}: {len(df):,} registros -> guardado en cache")
    return df


def get_mix_generacion(start_date: str,
                       end_date:   str,
                       time_trunc: str = "hour",
                       force_download: bool = False) -> pd.DataFrame:
    """
    Descarga (o carga desde cache) la generacion por tecnologia.

    Columnas resultantes (MW horarios):
      eolica, solar_fv, solar_termica, nuclear, hidraulica, gas_ccgt,
      renovable_mw, total_mw, pct_renovable (%)

    NOTA: gas_ccgt (ID 550) e hidraulica (ID 178) tienen granularidad 5-min
    en la API publica de ESIOS. La funcion los carga desde raw parquets locales
    (data/raw/esios/) y los resamplea a horario, garantizando cobertura completa.
    El encoding corrupto ('PenInsula') se maneja con .iloc[:,0].

    Guarda el resultado combinado en:
      data/processed/esios/mix_generacion_<start>_<end>.parquet
    """
    if not TOKEN_CONFIGURED:
        _phase_warning("get_mix_generacion")
        return pd.DataFrame()

    # Cache del mix combinado
    mix_cache = (_CACHE_DIR /
                 f"mix_generacion_{start_date.replace('-','')}_{end_date.replace('-','')}.parquet")

    if mix_cache.exists() and not force_download:
        print(f"[CACHE] Mix generacion: cargando desde {mix_cache.name}")
        return pd.read_parquet(mix_cache)

    print(f"[API] Descargando mix de generacion {start_date} -> {end_date}...")

    # --- 1. Descargar tecnologias desde la API, forzando frecuencia horaria ---
    frames = {}
    for tech_name, ind_id in INDICADORES_MIX.items():
        try:
            df_i = get_indicator(ind_id, start_date, end_date, time_trunc, force_download)
            if not df_i.empty:
                s = df_i["value"] if "value" in df_i.columns else df_i.iloc[:, 0]
                s = s[~s.index.duplicated(keep='first')]
                # Forzar resampleo a horario (por si la API devuelve 5-min)
                s = s.resample("h").mean()
                frames[tech_name] = s
        except Exception as e:
            print(f"  [WARN] {tech_name} (id={ind_id}): {e}")

    if not frames:
        return pd.DataFrame()

    mix = pd.DataFrame(frames).sort_index()

    # --- 2. ESTRATEGIA: Obtener generación desde API ESIOS ---
    # Todos los datos se descargan desde API REE (ESIOS)
    # No se usan archivos locales (fueron datos de investigación diagnosticados como incorrectos)
    
    # Para hidraulica: el indicador 178 ya está descargado en frames
    # Solo aplicar imputación si hay nulos
    if "hidraulica" in mix.columns:
        n_nulos_hidro = int(mix["hidraulica"].isna().sum())
        if n_nulos_hidro > 0:
            # Interpolar máximo 7 horas para llenar pequeños gaps
            mix["hidraulica"] = mix["hidraulica"].interpolate(method="time", limit=7)
            n_final = int(mix["hidraulica"].isna().sum())
            print(f"  [INTERP] hidraulica: {n_nulos_hidro} nulos interpolados (limit=7h) -> {n_final} nulos finales")

    # --- 3. Recalcular pct_renovable y total_mw con datos completos ---
    # IMPORTANTE: usar skipna=True para contar MW disponibles aunque haya algunos nulos
    # (No queremos que un valor nulo invalide toda la fila)
    
    renovables   = [c for c in ["eolica", "solar_fv", "solar_termica", "hidraulica"] if c in mix.columns]
    convencional = [c for c in ["nuclear", "gas_ccgt"] if c in mix.columns]
    
    # Suma con skipna=True (ignora NaN)
    mix["renovable_mw"]  = mix[renovables].sum(axis=1, skipna=True) if renovables else 0
    mix["convencional_mw"] = mix[convencional].sum(axis=1, skipna=True) if convencional else 0
    mix["total_mw"]      = mix["renovable_mw"] + mix["convencional_mw"]
    
    # pct_renovable: solo sobre total (evita división por 0)
    mix["pct_renovable"] = (
        mix["renovable_mw"] / mix["total_mw"].replace(0, float("nan")) * 100
    ).round(1)
    
    # Eliminar columna auxiliar convencional_mw para no contaminar output
    mix = mix.drop(columns=["convencional_mw"], errors="ignore")

    # Guardar mix combinado en cache
    mix.to_parquet(mix_cache)
    print(f"\n[OK] Mix generacion guardado: {len(mix):,} horas -> {mix_cache.name}")
    return mix


def get_sistema_context(start_date: str,
                        end_date:   str,
                        force_download: bool = False) -> pd.DataFrame:
    """
    Descarga (o carga desde cache) demanda, embalses e intercambios.

    Guarda el resultado en:
      data/processed/esios/sistema_context_<start>_<end>.parquet
    """
    if not TOKEN_CONFIGURED:
        _phase_warning("get_sistema_context")
        return pd.DataFrame()

    sis_cache = (_CACHE_DIR /
                 f"sistema_context_{start_date.replace('-','')}_{end_date.replace('-','')}.parquet")

    if sis_cache.exists() and not force_download:
        print(f"[CACHE] Contexto sistema: cargando desde {sis_cache.name}")
        return pd.read_parquet(sis_cache)

    print(f"[API] Descargando contexto del sistema {start_date} -> {end_date}...")

    frames = {}
    for name, ind_id in INDICADORES_SISTEMA.items():
        trunc = "day" if name == "embalses_gwh" else "hour"
        try:
            df_i = get_indicator(ind_id, start_date, end_date, trunc, force_download)
            if not df_i.empty:
                s = df_i["value"] if "value" in df_i.columns else df_i.iloc[:, 0]
                frames[name] = s[~s.index.duplicated(keep='first')]
        except Exception as e:
            print(f"  [WARN] {name} (id={ind_id}): {e}")

    if not frames:
        return pd.DataFrame()

    df_sis = pd.DataFrame(frames).sort_index()
    df_sis.to_parquet(sis_cache)
    print(f"[OK] Contexto sistema guardado: {len(df_sis):,} registros -> {sis_cache.name}")
    return df_sis


def merge_with_prices(df_prices: pd.DataFrame,
                      df_esios:  pd.DataFrame) -> pd.DataFrame:
    """Une el DataFrame OMIE con datos ESIOS por DatetimeIndex (join left)."""
    if df_esios.empty:
        return df_prices
    return df_prices.join(df_esios, how="left")


def esios_status() -> dict:
    """Devuelve el estado de la conexion ESIOS y del cache local."""
    cached_files = list(_CACHE_DIR.glob("*.parquet"))
    return {
        "fase": 2 if TOKEN_CONFIGURED else 1,
        "token_configured": TOKEN_CONFIGURED,
        "token_source": ".env" if _dot_env.exists() else "ESIOS_API_KEY env var",
        "libreria": "python-esios (datons/python-esios)",
        "cache_dir": str(_CACHE_DIR),
        "archivos_en_cache": len(cached_files),
    }


# --- Test / descarga inicial -------------------------------------------------
if __name__ == "__main__":
    status = esios_status()
    print("Estado ESIOS:")
    for k, v in status.items():
        print(f"  {k}: {v}")

    if TOKEN_CONFIGURED:
        print("\n[TEST] Descargando mix de generacion 2024 (guarda en cache)...")
        df_mix = get_mix_generacion("2024-01-01", "2024-12-31")
        if not df_mix.empty:
            print(df_mix.describe().round(1))

        print("\n[TEST] Descargando contexto del sistema 2024...")
        df_sis = get_sistema_context("2024-01-01", "2024-12-31")
        if not df_sis.empty:
            print(df_sis.describe().round(1))
    else:
        print("\n-> Proyecto en FASE 1. Configura el token para activar FASE 2.")
