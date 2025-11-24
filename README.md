# 🧬 FarmaEnvios – Azure Functions para Scraping Farmacéutico

## 📘 Descripción General
Este repositorio contiene un conjunto de **Azure Functions (Python)** diseñadas para automatizar el scraping de precios de distintas farmacias (**Farmacias San Pablo**, **Farmacias Especializadas**, **FarmaTodo**, etc.), como parte del ecosistema **FarmaEnvios**.

Las funciones se ejecutan sobre un **Plan Premium Linux**, utilizan **Playwright** para la automatización con Chromium y **BeautifulSoup/Requests** para extracciones más livianas.  
Los resultados se guardan como archivos CSV en **Azure Blob Storage**, alimentando flujos de datos en **Microsoft Fabric / Synapse** y dashboards de **Power BI**.

---

## 🧱 Estructura del Proyecto

```
scraping-farmacias/
│
├── function_app.py                # Registro de funciones HTTP (Farmacia, FarmaTodo, SanPablo)
├── scrapper_san_pablo.py          # Lógica Playwright (Farmacia San Pablo)
├── requirements.txt               # Dependencias del proyecto
├── startup.sh                     # Script de inicialización en Azure Premium
├── local.settings.json            # Variables de entorno locales
└── .venv/                         # Entorno virtual local (no se sube al repo)
```

---

## ⚙️ Funcionalidades Principales

| Función | Descripción | Tecnología principal |
|----------|--------------|----------------------|
| **scrapingFarmacia** | Scraping de *Farmacias Especializadas* mediante HTML parsing | Requests + BeautifulSoup |
| **scrapingFarmaTodo** | Extracción de precios desde *FarmaTodo* | Requests + Regex |
| **scrapingSanPablo** | Scraping de *Farmacia San Pablo* vía API + navegador sin cabecera | Playwright (Chromium) |

---

## 🚀 Ejecución Local

### 1. Crear y activar el entorno virtual
```bash
python -m venv .venv
.venv\Scripts\activate   # Windows
source .venv/bin/activate  # Linux/Mac
```

### 2. Instalar dependencias
```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 3. Ejecutar la Function App
```bash
func start
```

### 4. Probar desde el navegador o Postman
```
http://localhost:7071/api/scrapingSanPablo?upc_path=upc_list_1.json
```

---

## ☁️ Despliegue en Azure Premium

### 1. Publicar desde VS Code o CLI
```bash
func azure functionapp publish farma-function-prem
```

### 2. Verificar configuración
```bash
az webapp config show --name farma-function-prem --resource-group Farma-Envios --query appCommandLine
```

### 3. Script de arranque (`startup.sh`)
```bash
#!/bin/bash
echo "Instalando Playwright y Chromium..."
python3 -m playwright install chromium --with-deps > /tmp/playwright_install.log 2>&1
echo "Playwright instalado correctamente."
```

---

## 🧩 Variables de Entorno

Definidas en `local.settings.json` (modo local) o en el portal de Azure (Producción):

| Variable | Descripción |
|-----------|-------------|
| `FUNCTIONS_WORKER_RUNTIME` | Define el runtime de Azure Functions (`python`) |
| `AzureWebJobsStorage` | Cadena de conexión del almacenamiento general |
| `BLOB_CONNECTION` | Cadena de conexión al contenedor principal de blobs |
| `SCRAPER_DEBUG` | (opcional) Activa logs detallados si está en `true` |

**Ejemplo:**
```json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=https;AccountName=farmaenvios;AccountKey=xxxxx;EndpointSuffix=core.windows.net",
    "BLOB_CONNECTION": "DefaultEndpointsProtocol=https;AccountName=farmaenvios;AccountKey=xxxxx;EndpointSuffix=core.windows.net",
    "SCRAPER_DEBUG": "true"
  }
}
```

---

## 🪄 Estructura de Respuesta (API)

**Ejemplo de respuesta exitosa**
```json
{
  "status": "ok",
  "mensaje": "Lote upc_list_1.json procesado correctamente. Archivo subido a Blob Storage.",
  "registros": 52
}
```

**Ejemplo de error**
```json
{
  "status": "error",
  "mensaje": "BrowserType.launch_persistent_context: Executable doesn't exist ..."
}
```

---

## 🔍 Mantenimiento y Diagnóstico

**Consola Kudu / SSH:**  
[https://<nombre-funcion>.scm.azurewebsites.net/DebugConsole](https://<nombre-funcion>.scm.azurewebsites.net/DebugConsole)

**Verificar instalación de Playwright:**
```bash
ls -lah /tmp/playwright/chromium-1187/chrome-linux/
python3 -m playwright --version
```

**Revisar logs:**
```bash
cat /tmp/playwright_install.log | head -n 50
```

---

## 🧠 Buenas Prácticas

- Usar siempre rutas bajo `/tmp` (Azure Functions no permite escritura en `/home/site/wwwroot`).
- Evitar dependencias innecesarias: mantener `requirements.txt` limpio.
- Verificar `BLOB_CONNECTION` en Configuración de Aplicación antes del despliegue.
- Usar plan **Premium** o **Dedicated** (Playwright no funciona en consumo).
- Registrar los errores en **Application Insights** si la app lo permite.

---

## 🔄 Pruebas desde Synapse Pipeline

Si se invoca la Function desde un **pipeline de Synapse**, debe configurarse la actividad **Web** con los siguientes parámetros:

- **URL:**  
  `https://farma-function-prem.azurewebsites.net/api/scrapingSanPablo?upc_path=upc_list_1.json`
- **Método:** `GET`
- **Autenticación:** `Anonymous` (o Managed Identity si se habilita en el futuro)
- **Timeout:** `15–20 minutos` (Playwright puede tardar más en ejecución Premium)

---

