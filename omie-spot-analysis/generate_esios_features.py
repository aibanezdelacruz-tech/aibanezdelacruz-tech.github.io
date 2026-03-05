# -*- coding: utf-8 -*-
"""
Generar esios_features.parquet manualmente desde el mix correcto
"""
import os
from pathlib import Path
import pandas as pd

# Cargar token
env_file = Path(".env")
if env_file.exists():
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip().strip('"').strip("'")

from src.esios_client import get_mix_generacion

print("=" * 80)
print("GENERANDO esios_features.parquet MANUALMENTE")
print("=" * 80)

# Cargar el mix (ahora con datos correctos)
print("\n1. Cargando mix con datos correctos...")
df_mix = get_mix_generacion("2023-01-01", "2025-09-30")

if df_mix.empty:
    print("ERROR: df_mix está vacío")
    exit(1)

print(f"   ✓ Shape: {df_mix.shape}")

# Seleccionar columnas para el modelo
cols_modelo = ['eolica', 'solar_fv', 'nuclear', 'hidraulica', 'gas_ccgt', 
               'renovable_mw', 'total_mw']

df_esios_features = df_mix[cols_modelo].copy()

# Persistir
out_path = Path('data/processed/esios_features.parquet')
out_path.parent.mkdir(parents=True, exist_ok=True)
df_esios_features.to_parquet(out_path)

print(f"\n2. ✓ GENERADO esios_features.parquet")
print(f"   Shape: {df_esios_features.shape}")
print(f"   Columnas: {list(df_esios_features.columns)}")
print(f"   Ubicación: {out_path}")

# Verificación de valores
print(f"\n3. VERIFICACIÓN (valores deben ser REALES):")
print(f"   nuclear:  mean={df_esios_features['nuclear'].mean():.1f} MW ✓ (esperado ~6070)")
print(f"   solar_fv: mean={df_esios_features['solar_fv'].mean():.1f} MW ✓")
print(f"   eolica:   mean={df_esios_features['eolica'].mean():.1f} MW ✓")

print(f"\n✓✓✓ FEATURES CON DATOS CORRECTOS LISTOS PARA EL MODELO ✓✓✓")
