#!/bin/bash
echo "=== [Startup] Inicializando entorno Playwright ==="

# Asegurar que el cache está en una ubicación accesible
export PLAYWRIGHT_BROWSERS_PATH=/home/site/wwwroot/.playwright
export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=0

# Crear carpeta de destino si no existe
mkdir -p /home/site/wwwroot/.playwright

echo "=== [Startup] Instalando Chromium si no está presente ==="
python -m playwright install --with-deps chromium

echo "=== [Startup] Listo. Iniciando Azure Functions runtime ==="
python -m azure_functions_worker
