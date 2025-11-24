import os
import sys
import json
import csv
import re
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from time import sleep
from playwright.sync_api import sync_playwright

# ============================================================
# CONFIG AZURE (Linux Premium)
# ============================================================

TMP_PLAYWRIGHT = "/tmp/playwright"
os.environ["PLAYWRIGHT_BROWSERS_PATH"] = TMP_PLAYWRIGHT

def instalar_chromium():
    """
    Instala Chromium únicamente en Azure (Linux). Se omite en Windows para desarrollo local.
    """
    if os.name == "nt":
        logging.info("Windows detectado: se omite instalación de Chromium.")
        return

    logging.info("Instalando Chromium en Azure Function…")

    result = subprocess.run(
        [sys.executable, "-m", "playwright", "install", "--with-deps", "chromium"],
        env={**os.environ, "PLAYWRIGHT_BROWSERS_PATH": TMP_PLAYWRIGHT},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    with open("/tmp/playwright_install.log", "w") as f:
        f.write(result.stdout)

    if result.returncode != 0:
        logging.error("Error instalando Chromium:\n" + result.stdout)
        raise RuntimeError("No se pudo instalar Chromium en Azure")

    logging.info("Chromium instalado correctamente.")


def find_headless_shell():
    """
    Busca el ejecutable `headless_shell` dentro de /tmp/playwright.
    """
    for root, dirs, files in os.walk(TMP_PLAYWRIGHT):
        if "headless_shell" in files:
            return os.path.join(root, "headless_shell")
    raise FileNotFoundError("headless_shell no encontrado en /tmp/playwright")


# ============================================================
# LOGGING
# ============================================================

DEBUG = os.getenv("SCRAPER_DEBUG", "").strip().lower() in ("1", "true", "yes", "on", "debug")
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fsp-scraper")


# ============================================================
# CONSTANTES
# ============================================================

BASE_WEB = "https://www.farmaciasanpablo.com.mx"
API_HOST = "https://api.farmaciasanpablo.com.mx"
SITE_ID = "fsp"
PREFIX = "/rest/v2"
CURR = "MXN"
LANG = "es_MX"

COMMON_HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "es-MX,es;q=0.9",
    "Origin": BASE_WEB,
    "Referer": BASE_WEB + "/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "X-Requested-With": "XMLHttpRequest",
}


# ============================================================
# UTILITARIOS
# ============================================================

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def num(x):
    if x is None or isinstance(x, bool):
        return None
    try:
        if isinstance(x, (int, float)):
            return float(x)
        m = re.search(r"([\d.,]+)", str(x))
        return float(m.group(1).replace(",", "")) if m else None
    except Exception:
        return None


def money(v):
    v = num(v)
    return "" if v is None else f"{v:.2f}"


def write_rows(rows, out_csv):
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "UPC",
        "Precio sin promoción",
        "Precio con promoción",
        "Nombre del producto",
        "Fecha Scrapping",
    ]
    new = not Path(out_csv).exists()
    with open(out_csv, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        if new:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def clean_digits(s):
    return re.sub(r"\D", "", str(s or ""))


# ============================================================
# API OCC (misma lógica robusta del primer script)
# ============================================================

class OCC:
    def __init__(self, context):
        self.req = context.request

    def search(self, q):
        url = f"{API_HOST}{PREFIX}/{SITE_ID}/products/search"
        params = {
            "query": q,
            "curr": CURR,
            "lang": LANG,
            "pageSize": "24",
            "currentPage": "0",
            "fields": "products(code,name)"
        }
        r = self.req.get(url, params=params, headers=COMMON_HEADERS, timeout=15000)
        if not r.ok:
            return []
        try:
            return r.json().get("products") or []
        except Exception:
            return []

    def detail(self, code):
        url = f"{API_HOST}{PREFIX}/{SITE_ID}/products/{code}"
        params = {"fields": "FULL", "curr": CURR, "lang": LANG}
        r = self.req.get(url, params=params, headers=COMMON_HEADERS, timeout=15000)
        if not r.ok:
            return {}
        try:
            return r.json()
        except Exception:
            return {}


def upc_matches(detail_json, upc):
    t = clean_digits(upc)
    if not t:
        return False

    # Campos directos
    for k in ("gtin", "ean", "upc", "sku", "visualCode"):
        if clean_digits(detail_json.get(k)) == t:
            return True

    # Listas
    for k in ("eans", "gtins", "upcs"):
        vals = detail_json.get(k) or []
        try:
            for v in vals:
                if clean_digits(v) == t:
                    return True
        except Exception:
            pass

    # Classifications
    for cl in detail_json.get("classifications", []) or []:
        for feat in cl.get("features", []) or []:
            for val in feat.get("featureValues", []) or []:
                if clean_digits(val.get("value")) == t:
                    return True
            if clean_digits(feat.get("value")) == t:
                return True

    return False


# ============================================================
# CARRITO (misma lógica robusta del primer script)
# ============================================================

class Cart:
    def __init__(self, context):
        self.req = context.request

    def create(self):
        base = f"{API_HOST}{PREFIX}/{SITE_ID}/users/anonymous/carts"

        r = self.req.post(base, params={"lang": LANG, "curr": CURR}, headers=COMMON_HEADERS, timeout=15000)
        if not r.ok:
            return None

        try:
            j = r.json()
            guid = j.get("guid")
            code = j.get("code")
        except Exception:
            return None

        if not guid:
            r2 = self.req.get(base, params={"fields": "DEFAULT", "lang": LANG, "curr": CURR},
                              headers=COMMON_HEADERS, timeout=15000)
            if r2.ok:
                try:
                    arr = r2.json()
                    if isinstance(arr, list) and arr:
                        guid = arr[0].get("guid") or arr[0].get("code")
                except Exception:
                    pass

        if not guid:
            r3 = self.req.get(f"{base}/current", params={"fields": "DEFAULT", "lang": LANG, "curr": CURR},
                              headers=COMMON_HEADERS, timeout=15000)
            if r3.ok:
                try:
                    cur = r3.json()
                    guid = cur.get("guid") or cur.get("code")
                except Exception:
                    pass

        return guid or code or None

    def add_entry(self, cart_id, code, qty=1):
        url = f"{API_HOST}{PREFIX}/{SITE_ID}/users/anonymous/carts/{cart_id}/entries"
        headers = {**COMMON_HEADERS, "Content-Type": "application/json"}
        body = json.dumps({"product": {"code": code}, "quantity": qty})
        r = self.req.post(url, params={"lang": LANG, "curr": CURR}, data=body, headers=headers, timeout=15000)
        return r.ok

    def get_prices(self, cart_id, entry_idx=0):
        fields = (
            "entries(entryNumber,product(code,name),"
            "basePrice(value,formattedValue),totalPrice(value,formattedValue))"
        )
        url = f"{API_HOST}{PREFIX}/{SITE_ID}/users/anonymous/carts/{cart_id}"
        params = {"fields": fields, "lang": LANG, "curr": CURR}

        r = self.req.get(url, params=params, headers=COMMON_HEADERS, timeout=15000)
        if not r.ok:
            return None

        try:
            j = r.json()
            ents = j.get("entries") or []
            if not ents:
                return None
            e = ents[entry_idx]
            base = num((e.get("basePrice") or {}).get("value"))
            total = num((e.get("totalPrice") or {}).get("value"))
            name = ((e.get("product") or {}).get("name") or "").strip()
            return base, total, name
        except Exception:
            return None

    def remove(self, cart_id, entry_idx=0):
        url = (
            f"{API_HOST}{PREFIX}/{SITE_ID}/users/anonymous/carts/"
            f"{cart_id}/entries/{entry_idx}"
        )
        try:
            self.req.delete(url, headers=COMMON_HEADERS, timeout=10000)
        except Exception:
            pass


# ============================================================
# SCRAPING PRINCIPAL (UNIFICADO)
# ============================================================

def main(upc_path="upc_list.json", out_csv="/tmp/salida_san_pablo.csv", headed=False):

    instalar_chromium()
    chromium_exe = find_headless_shell()

    with open(upc_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        upcs = data["upcs"] if isinstance(data, dict) else data

    rows = []

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir="/tmp/user_data_cart",
            executable_path=chromium_exe,
            headless=not headed,
            viewport={"width": 1280, "height": 800},
            locale="es-MX",
            timezone_id="America/Mexico_City",
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        occ = OCC(context)
        cart = Cart(context)

        cart_id = cart.create()
        if not cart_id:
            raise RuntimeError("No se pudo crear carrito anónimo")

        for upc in upcs:

            try:
                logger.info(f"Procesando UPC {upc}")

                products = occ.search(upc) or occ.search(f":relevance:freeText:{upc}")
                if not products:
                    rows.append({
                        "UPC": upc,
                        "Precio sin promoción": "-",
                        "Precio con promoción": "-",
                        "Nombre del producto": "No encontrado",
                        "Fecha Scrapping": now_str()
                    })
                    continue

                picked = None
                detail = {}

                for pdt in products:
                    code = pdt.get("code")
                    if not code:
                        continue
                    dj = occ.detail(code)
                    if dj and upc_matches(dj, upc):
                        picked, detail = pdt, dj
                        break

                if not picked:
                    rows.append({
                        "UPC": upc,
                        "Precio sin promoción": "-",
                        "Precio con promoción": "-",
                        "Nombre del producto": "No encontrado",
                        "Fecha Scrapping": now_str()
                    })
                    continue

                code = picked.get("code")
                name = (detail.get("name") or picked.get("name") or "").strip()

                if not cart.add_entry(cart_id, code):
                    rows.append({
                        "UPC": upc,
                        "Precio sin promoción": "-",
                        "Precio con promoción": "-",
                        "Nombre del producto": name,
                        "Fecha Scrapping": now_str()
                    })
                    continue

                sleep(0.2)
                got = cart.get_prices(cart_id, entry_idx=0)
                if not got:
                    sleep(0.4)
                    got = cart.get_prices(cart_id, entry_idx=0)

                if not got:
                    rows.append({
                        "UPC": upc,
                        "Precio sin promoción": "-",
                        "Precio con promoción": "-",
                        "Nombre del producto": name,
                        "Fecha Scrapping": now_str()
                    })
                    cart.remove(cart_id, entry_idx=0)
                    continue

                base, total, name2 = got
                if name2:
                    name = name2

                promo = (total if (total is not None and base is not None and total < base - 1e-9) else None)

                rows.append({
                    "UPC": upc,
                    "Precio sin promoción": money(base),
                    "Precio con promoción": money(promo),
                    "Nombre del producto": name,
                    "Fecha Scrapping": now_str(),
                })

                cart.remove(cart_id, entry_idx=0)

            except Exception as e:
                logger.exception(f"Error procesando UPC {upc}")
                rows.append({
                    "UPC": upc,
                    "Precio sin promoción": "-",
                    "Precio con promoción": "-",
                    "Nombre del producto": f"Error general: {e}",
                    "Fecha Scrapping": now_str()
                })

        context.close()

    write_rows(rows, out_csv)
    logger.info(f"Scraping completado: {len(upcs)} UPCs procesados")
    logger.info(f"Resultados guardados en: {out_csv}")
