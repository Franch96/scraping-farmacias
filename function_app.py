import azure.functions as func
import logging
import requests
from bs4 import BeautifulSoup
import json
import os
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import concurrent.futures
import re
from scrapper_san_pablo import main as scraping_san_pablo
from pathlib import Path

# --- Limpieza de texto de precio ---
def limpiar_precio(texto):
    texto = texto.replace("$", "").replace(",", "").replace("MXN", "").replace(" ", "").strip()
    try:
        return float(texto)
    except:
        return None

# --- Cabeceras HTTP ---
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/"
}

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
__all__ = ["app"]

# =================================================================
# 🔹 Scraping Farmacias Especializadas
# =================================================================
@app.route(route="scrapingFarmacia")

def scrapingFarmacia(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Scraping Farmacias Especializadas iniciado...')

    try:
        # --- 1. Conexión a Blob Storage ---
        blob_connection = os.environ["BLOB_CONNECTION"]
        container_name = "file-system-farma-envios"
        blob_name_in = "codigo_barra_scrapping.csv"

        blob_service = BlobServiceClient.from_connection_string(blob_connection)
        blob_client_in = blob_service.get_blob_client(container=container_name, blob=blob_name_in)

        # --- 2. Leer CSV de entrada desde blob ---
        stream = blob_client_in.download_blob()
        df = pd.read_csv(stream)
        codigos = df["Barra"].astype(str).tolist()
        BASE_URL = "https://www.farmaciasespecializadas.com/catalogsearch/result/?q="

        # --- 3. Función para obtener precio de un producto ---
        def obtener_precio(codigo):
            try:
                search_url = BASE_URL + codigo
                resp = requests.get(search_url, headers=HEADERS, timeout=20)
                if resp.status_code != 200:
                    return {"Barra": codigo, "Precio": None}

                soup = BeautifulSoup(resp.text, "html.parser")
                link_producto = soup.find("a", class_="product-item-link")
                if link_producto and link_producto.get("href"):
                    product_url = link_producto["href"]
                else:
                    return {"Barra": codigo, "Precio": None}

                resp2 = requests.get(product_url, headers=HEADERS, timeout=20)
                if resp2.status_code != 200:
                    return {"Barra": codigo, "Precio": None}

                soup2 = BeautifulSoup(resp2.text, "html.parser")

                # 1. data-price-amount
                tag = soup2.find("span", attrs={"data-price-amount": True})
                if tag:
                    return {"Barra": codigo, "Precio": float(tag["data-price-amount"])}

                # 2. itemprop="price"
                meta_price = soup2.find("meta", itemprop="price")
                if meta_price and meta_price.get("content"):
                    return {"Barra": codigo, "Precio": limpiar_precio(meta_price["content"])}

                # 3. Clases comunes
                tag = soup2.find("span", class_="price")
                if tag:
                    return {"Barra": codigo, "Precio": limpiar_precio(tag.get_text(strip=True))}

                tag = soup2.find("span", class_="special-price")
                if tag:
                    return {"Barra": codigo, "Precio": limpiar_precio(tag.get_text(strip=True))}

                # 4. Fallback con regex
                match = re.search(r"\$\s?[\d\.,]+", soup2.text)
                if match:
                    return {"Barra": codigo, "Precio": limpiar_precio(match.group())}

                return {"Barra": codigo, "Precio": None}

            except Exception as e:
                logging.warning(f"Error con código {codigo}: {e}")
                return {"Barra": codigo, "Precio": None}

        # --- 4. Ejecutar scraping concurrente ---
        resultados = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futuros = {executor.submit(obtener_precio, c): c for c in codigos}
            for future in concurrent.futures.as_completed(futuros):
                resultados.append(future.result())

        # --- 5. Generar DataFrame de salida ---
        df_out = pd.DataFrame(resultados, columns=["Barra", "Precio"])
        df_out["Fecha"] = datetime.now().strftime("%Y-%m-%d")
        df_out["Origen"] = "Farmacias Especializadas"

        # --- 6. Exportar CSV al blob ---
        csv_buffer = io.StringIO()
        df_out.to_csv(csv_buffer, index=False)
        blob_name_out = f"Scrapping/Farmacias_Especializadas/precios_farmacias_especializadas_{datetime.now().strftime('%Y%m%d')}.csv"

        blob_client_out = blob_service.get_blob_client(container=container_name, blob=blob_name_out)
        blob_client_out.upload_blob(csv_buffer.getvalue(), overwrite=True)

        # --- 7. Retornar respuesta ---
        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "mensaje": f"Archivo {blob_name_out} generado en contenedor {container_name}",
                "registros": len(resultados)
            }, ensure_ascii=False),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error en scrapingFarmacia: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "error", "mensaje": str(e)}),
            mimetype="application/json",
            status_code=500
        )

# =================================================================
# 🔹 Scraping FarmaTodo
# =================================================================
@app.route(route="scrapingFarmaTodo")
def scrapingFarmaTodo(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Scraping FarmaTodo iniciado...')

    try:
        blob_connection = os.environ["BLOB_CONNECTION"]
        container_name = "file-system-farma-envios"
        blob_name_in = "codigo_barra_scrapping.csv"

        blob_service = BlobServiceClient.from_connection_string(blob_connection)

        blob_client_in = blob_service.get_blob_client(container=container_name, blob=blob_name_in)
        stream = blob_client_in.download_blob()
        df = pd.read_csv(stream)

        codigos = df["Barra"].astype(str).tolist()
        BASE_URL = "https://www.farmatodo.com.mx/"

        def obtener_precio(codigo):
            url = BASE_URL + codigo
            headers = {"User-Agent": "Mozilla/5.0"}
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                if resp.status_code != 200:
                    return {"Barra": codigo, "Precio": None}

                soup = BeautifulSoup(resp.text, "html.parser")
                texto = soup.get_text()

                # Buscar rango tipo $122.00–$130.00
                match = re.search(r"\$[0-9\.,]+\s*–\s*\$[0-9\.,]+", texto)
                if match:
                    rango = match.group(0)
                    minimo = rango.split("–")[0]
                    return {"Barra": codigo, "Precio": limpiar_precio(minimo)}

                # Fallback: precio único
                unico = re.search(r"\$([0-9\.,]+)", texto)
                if unico:
                    return {"Barra": codigo, "Precio": limpiar_precio(unico.group(1))}

                return {"Barra": codigo, "Precio": None}
            except Exception:
                return {"Barra": codigo, "Precio": None}

        resultados = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futuros = {executor.submit(obtener_precio, c): c for c in codigos}
            for future in concurrent.futures.as_completed(futuros):
                resultados.append(future.result())

        df_out = pd.DataFrame(resultados, columns=["Barra", "Precio"])
        df_out["Fecha"] = datetime.now().strftime("%Y-%m-%d")
        df_out["Origen"] = "FarmaTodo"

        csv_buffer = io.StringIO()
        df_out.to_csv(csv_buffer, index=False)

        blob_name_out = f"Scrapping/FarmaTodo/precios_farmatodo_{datetime.now().strftime('%Y%m%d')}.csv"
        blob_client_out = blob_service.get_blob_client(container=container_name, blob=blob_name_out)
        blob_client_out.upload_blob(csv_buffer.getvalue(), overwrite=True)

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "mensaje": f"Archivo {blob_name_out} generado en contenedor {container_name}",
                "registros": len(resultados)
            }, ensure_ascii=False),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error en scrapingFarmaTodo: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "error", "mensaje": str(e)}),
            mimetype="application/json",
            status_code=500
        )
    
# =================================================================
# 🔹 Scraping SanPablo
# =================================================================
@app.route(route="scrapingSanPablo")
def scrapingSanPablo(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Scraping Farmacia San Pablo iniciado...')

    try:
        # Archivo (solo nombre)
        file_name = req.params.get("upc_path") or "upc_list_1.json"

        # Ruta fija dentro del Blob
        blob_path = f"UPCs San Pablo/{file_name}"

        logging.info(f"Procesando lote desde blob: {blob_path}")

        # Conexión Blob
        blob_connection = os.environ["BLOB_CONNECTION"]
        container_name = "file-system-farma-envios"
        blob_service = BlobServiceClient.from_connection_string(blob_connection)

        # Descargar el archivo JSON desde Blob a /tmp
        local_upc_file = f"/tmp/{file_name}"
        blob_client_in = blob_service.get_blob_client(container=container_name, blob=blob_path)

        with open(local_upc_file, "wb") as f:
            f.write(blob_client_in.download_blob().readall())

        logging.info(f"Archivo UPC descargado localmente en {local_upc_file}")

        # Generar CSV temporal
        out_csv = f"/tmp/salida_{Path(file_name).stem}.csv"

        # Ejecutar scraper
        scraping_san_pablo(
            upc_path=local_upc_file,
            out_csv=out_csv,
            headed=False
        )

        # Subir resultado
        blob_name_out = (
            f"Scrapping/SanPablo/precios_{Path(file_name).stem}_"
            f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        blob_client_out = blob_service.get_blob_client(container=container_name, blob=blob_name_out)
        with open(out_csv, "rb") as data:
            blob_client_out.upload_blob(data, overwrite=True)

        # Limpieza
        os.remove(out_csv)
        os.remove(local_upc_file)

        return func.HttpResponse(
            json.dumps({
                "status": "ok",
                "mensaje": f"Lote {file_name} procesado correctamente. Archivo {blob_name_out} subido."
            }, ensure_ascii=False),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error en scrapingSanPablo: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"status": "error", "mensaje": str(e)}),
            mimetype="application/json",
            status_code=500
        )
