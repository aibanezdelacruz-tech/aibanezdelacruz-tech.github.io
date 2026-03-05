#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Re-run all notebooks with CORRECTED ESIOS data (2025-03-05)
"""
import subprocess
import sys
from pathlib import Path

notebooks = [
    "notebooks/00_setup.ipynb",
    "notebooks/01_data_extraction.ipynb",
    "notebooks/02_eda.ipynb",
    "notebooks/03_forecasting.ipynb",
    "notebooks/04_advanced_analysis.ipynb",
]

print("=" * 80)
print("RE-EJECUTANDO NOTEBOOKS CON DATOS ESIOS CORREGIDOS")
print("=" * 80)

for nb in notebooks:
    nb_path = Path(nb)
    if not nb_path.exists():
        print(f"\n[SKIP] {nb}: NO EXISTE")
        continue
    
    print(f"\n{'=' * 80}")
    print(f"[RUN] {nb}")
    print(f"{'=' * 80}")
    
    cmd = [
        "python", "-m", "jupyter", "nbconvert",
        "--to", "notebook",
        "--execute",
        "--inplace",
        str(nb_path)
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=False, timeout=600)
        if result.returncode == 0:
            print(f"[OK] {nb}: SUCCESS")
        else:
            print(f"[FAIL] {nb}: FAILED (Exit code: {result.returncode})")
    except subprocess.TimeoutExpired:
        print(f"[TIMEOUT] {nb}: TIMEOUT (10 minutos)")
    except Exception as e:
        print(f"[ERROR] {nb}: {e}")

print("\n" + "="*80)
print("EJECUCION COMPLETADA")
print("="*80)
