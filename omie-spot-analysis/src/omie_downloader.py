"""
omie_downloader.py
------------------
Descarga automatica de archivos del Operador del Mercado Iberico de Energia (OMIE).

Replicamos la metodologia de OMIE-starter:
  - URL base: https://www.omie.es/en/file-download
  - Producto: marginalpdbc (precio marginal mercado diario)
  - Formatos disponibles en OMIE File Access: https://www.omie.es/en/file-access-list

Productos disponibles (seleccionar en PRODUCT):
  'marginalpdbc'   : Precio marginal mercado diario
  'curva_pbc_uof'  : Curva de oferta del mercado diario (volumen + precio por unidad)
  'curva_pbc'      : Curva de compra del mercado diario

Uso rapido:
  python src/omie_downloader.py --start 2025-01-01 --end 2025-01-31
"""

import requests
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta

# --- Configuracion -----------------------------------------------------------
PRODUCT = "marginalpdbc"
BASE_URL = "https://www.omie.es/en/file-download"
DEFAULT_OUT = Path(__file__).parent.parent / "data" / "raw"
DELAY_BETWEEN_REQUESTS = 2.0   # segundos (respeta rate-limit del servidor OMIE)


def build_omie_url(date: datetime, product: str = PRODUCT) -> str:
    """
    Construye la URL de descarga de OMIE para una fecha concreta.

    Formato OMIE: https://www.omie.es/en/file-download?parents=PRODUCT&filename=PRODUCT_YYYYMMDD.1
    (metodologia identica a la del workbook 1_Download de OMIE-starter)
    """
    date_str = date.strftime("%Y%m%d")
    filename  = f"{product}_{date_str}.1"
    return f"{BASE_URL}?parents={product}&filename={filename}"


def download_day(date: datetime,
                 out_dir: Path = DEFAULT_OUT,
                 product: str  = PRODUCT,
                 verbose: bool  = True) -> Path | None:
    """
    Descarga el fichero OMIE de un dia concreto y lo guarda en out_dir.

    Parametros
    ----------
    date    : fecha a descargar
    out_dir : carpeta de salida (se crea si no existe)
    product : identificador del producto OMIE
    verbose : si imprime progreso

    Retorna
    -------
    Path al fichero guardado, o None si fallo.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{date.strftime('%Y%m%d')}.txt"

    if out_file.exists():
        if verbose:
            print(f"  [SKIP] {date.date()} ya existe")
        return out_file

    url = build_omie_url(date, product)
    try:
        resp = requests.get(url, timeout=20)
        if resp.status_code == 200 and len(resp.content) > 200:
            out_file.write_bytes(resp.content)
            if verbose:
                print(f"  [OK]   {date.date()} -> {out_file.name}  ({len(resp.content):,} bytes)")
            return out_file
        else:
            if verbose:
                print(f"  [WARN] {date.date()} -> HTTP {resp.status_code} (datos no disponibles aun)")
            return None
    except requests.RequestException as e:
        if verbose:
            print(f"  [ERR]  {date.date()} -> {e}")
        return None


def download_range(start: datetime,
                   end:   datetime,
                   out_dir: Path = DEFAULT_OUT,
                   product: str  = PRODUCT) -> list[Path]:
    """
    Descarga todos los dias en el rango [start, end] (inclusive).
    El delay entre peticiones evita saturar el servidor de OMIE.

    Ejemplo:
      download_range(datetime(2025, 1, 1), datetime(2025, 3, 1))
    """
    total_days = (end - start).days + 1
    print(f"[INFO] Descargando {total_days} dias: {start.date()} -> {end.date()}")
    print(f"       Producto: {product}")
    print(f"       Destino : {out_dir}")
    print()

    downloaded = []
    current = start
    while current <= end:
        result = download_day(current, out_dir, product)
        if result:
            downloaded.append(result)
        time.sleep(DELAY_BETWEEN_REQUESTS)
        current += timedelta(days=1)

    print()
    print(f"[OK] Descargados: {len(downloaded)} / {total_days} dias")
    return downloaded


# --- CLI ---------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Descarga automatica de datos OMIE (marginalpdbc)"
    )
    parser.add_argument(
        "--start", required=True,
        help="Fecha inicio en formato YYYY-MM-DD"
    )
    parser.add_argument(
        "--end", default=None,
        help="Fecha fin en formato YYYY-MM-DD (por defecto = hoy)"
    )
    parser.add_argument(
        "--out", default=str(DEFAULT_OUT),
        help="Directorio de salida (por defecto: data/raw/)"
    )
    parser.add_argument(
        "--product", default=PRODUCT,
        help=f"Producto OMIE (por defecto: {PRODUCT})"
    )
    args = parser.parse_args()

    start_dt = datetime.strptime(args.start, "%Y-%m-%d")
    end_dt   = datetime.strptime(args.end,   "%Y-%m-%d") if args.end else datetime.today()

    download_range(start_dt, end_dt, Path(args.out), args.product)
