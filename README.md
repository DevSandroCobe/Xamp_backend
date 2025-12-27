# 🔧 Backend - Sistema de Migración y Generación de Actas PDF

Backend robusto para la migración automatizada de datos desde SAP (HANA/SQL Server) y generación de actas de despacho en PDF.  Desarrollado con FastAPI y diseñado para integrarse con el frontend Vue.js.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?logo=fastapi&logoColor=white)
![WeasyPrint](https://img.shields.io/badge/WeasyPrint-PDF-FF6347)

---

## 📋 Tabla de Contenidos

- [Características](#-características)
- [Tecnologías](#%EF%B8%8F-tecnologías-utilizadas)
- [Requisitos Previos](#-requisitos-previos)
- [Instalación](#-instalación)
- [Configuración](#-configuración)
- [Ejecución](#%EF%B8%8F-ejecución)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [API Endpoints](#-api-endpoints)
- [Logging y Monitoreo](#-logging-y-monitoreo)
- [Despliegue](#-despliegue)
- [Troubleshooting](#-troubleshooting)

---

## ✨ Características

### 🔄 Migración de Datos
- ✅ Extracción de datos desde SAP HANA y SQL Server
- ✅ Transformación y limpieza de datos (deduplicación, normalización)
- ✅ Control de fechas y rangos de migración
- ✅ Logs detallados de operaciones y errores
- ✅ Validación de integridad de datos
- ✅ Reintentos automáticos en caso de fallos

### 📄 Generación de PDFs
- ✅ Creación de actas de despacho profesionales
- ✅ Plantillas HTML+CSS personalizables con Jinja2
- ✅ Nombres de archivo únicos para evitar sobrescritura
- ✅ Organización por fechas en carpetas
- ✅ Generación bajo demanda vía API
- ✅ Soporte para gráficos y tablas complejas

### 🌐 API REST
- ✅ Documentación automática con Swagger/OpenAPI
- ✅ Autenticación y autorización (si implementado)
- ✅ Respuestas estructuradas y manejo de errores
- ✅ CORS configurado para integración con frontend
- ✅ Validación de datos con Pydantic

---

## 🏗️ Tecnologías Utilizadas

| Componente         | Tecnología                                 | Versión | Uso |
|--------------------|--------------------------------------------|---------|-----|
| **Lenguaje**       | Python                                     | 3.10+   | Backend principal |
| **Framework API**  | FastAPI                                    | 0.100+  | API REST moderna |
| **Servidor ASGI**  | Uvicorn                                    | 0.23+   | Servidor de aplicaciones |
| **PDF Engine**     | WeasyPrint                                 | 59.0+   | Generación de PDFs |
| **Plantillas**     | Jinja2                                     | 3.1+    | Templates HTML |
| **DB - SQL Server**| pyodbc                                     | 4.0+    | Conexión SQL Server |
| **DB - SAP HANA**  | pyhdb / hdbcli                             | -       | Conexión SAP HANA |
| **Validación**     | Pydantic                                   | 2.0+    | Validación de datos |
| **Logging**        | logging (RotatingFileHandler)              | Stdlib  | Registro de logs |
| **Variables Env**  | python-dotenv                              | 1.0+    | Gestión de configuración |

---

## 📦 Requisitos Previos

### Software Base
- **Python**:  3.10 o superior ([Descargar](https://www.python.org/downloads/))
- **pip**: 23.x o superior (incluido con Python)
- **Git**: Para clonar el repositorio

### Bases de Datos
- Acceso a **SAP HANA** o **SQL Server** con credenciales válidas
- Drivers de conexión instalados (ver instalación)

### Dependencias del Sistema para WeasyPrint

**Windows:**
- [GTK for Windows Runtime](https://github.com/tschoonj/GTK-for-Windows-Runtime-Environment-Installer)
  
**Linux (Debian/Ubuntu):**
```bash
sudo apt update
sudo apt install -y \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libpangoft2-1.0-0 \
    libcairo2 \
    libgdk-pixbuf2.0-0 \
    libffi-dev \
    shared-mime-info
```

**macOS:**
```bash
brew install cairo pango gdk-pixbuf libffi
```

---

## ⚙️ Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/DevSandroCobe/Xamp_backend.git
cd Xamp_backend
```

### 2. Crear Entorno Virtual

**Windows:**
```bash
python -m venv env
env\Scripts\activate
```

**Linux/macOS:**
```bash
python3 -m venv env
source env/bin/activate
```

### 3. Instalar Dependencias Python

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Instalar Drivers de Base de Datos

**Para SQL Server (pyodbc):**
- Descarga e instala el [ODBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

**Para SAP HANA:**
```bash
pip install hdbcli
# O si usas pyhdb:
pip install pyhdb
```

---

## 🔧 Configuración

### 1. Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto: 

```env
# Configuración General
APP_NAME=Xamp Backend
APP_ENV=development
DEBUG=True

# API
API_HOST=0.0.0.0
API_PORT=8000
API_RELOAD=True

# Base de Datos - SQL Server
SQLSERVER_DRIVER=ODBC Driver 17 for SQL Server
SQLSERVER_SERVER=your-server.database.windows.net
SQLSERVER_DATABASE=your_database
SQLSERVER_USER=your_username
SQLSERVER_PASSWORD=your_password

# Base de Datos - SAP HANA
HANA_HOST=hana-host.com
HANA_PORT=30015
HANA_USER=your_hana_user
HANA_PASSWORD=your_hana_password

# Rutas
PDF_OUTPUT_DIR=./output/pdfs
LOGS_DIR=./logs
TEMPLATES_DIR=./templates

# Logging
LOG_LEVEL=INFO
LOG_MAX_BYTES=10485760
LOG_BACKUP_COUNT=5

# CORS (Frontend URLs permitidas)
CORS_ORIGINS=http://localhost:5173,http://localhost:3000

# Seguridad (si aplica)
SECRET_KEY=your-secret-key-here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

### 2. Configuración de Bases de Datos

Edita los archivos de configuración en `Config/` si es necesario: 
- `Config/database.py` - Cadenas de conexión
- `Config/settings.py` - Configuraciones generales

---

## 🏃‍♂️ Ejecución

### Modo Desarrollo

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Modo Producción

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Con Docker (Opcional)

```dockerfile
# Dockerfile ejemplo
FROM python:3.10-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    libpango-1.0-0 libcairo2 libgdk-pixbuf2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main: app", "--host", "0.0.0.0", "--port", "8000"]
```

```bash
docker build -t xamp-backend . 
docker run -p 8000:8000 --env-file .env xamp-backend
```

---

## 📁 Estructura del Proyecto

```
Xamp_backend/
├── main.py                  # Punto de entrada FastAPI
├── requirements.txt         # Dependencias Python
├── . env                     # Variables de entorno (no versionado)
├── .gitignore
│
├── Config/                  # Configuraciones
│   ├── __init__.py
│   ├── settings.py          # Configuración general
│   └── database.py          # Configuración de BD
│
├── Conexion/                # Gestión de conexiones
│   ├── __init__.py
│   ├── sql_server.py        # Conexión SQL Server
│   └── sap_hana.py          # Conexión SAP HANA
│
├── Migrador/                # Lógica de migración
│   ├── __init__.py
│   ├── migrador.py          # Clase principal de migración
│   ├── extractors.py        # Extractores de datos
│   ├── transformers.py      # Transformadores de datos
│   └── loaders.py           # Cargadores de datos
│
├── generador_pdf/           # Generación de PDFs
│   ├── __init__.py
│   ├── generator.py         # Lógica de generación
│   ├── templates/           # Plantillas HTML
│   │   ├── acta_traslado.html
│   │   └── acta_venta.html
│   └── styles/              # Estilos CSS
│       └── acta_styles.css
│
├── Utils/                   # Utilidades
│   ├── __init__.py
│   ├── logger.py            # Configuración de logging
│   ├── validators.py        # Validaciones
│   └── helpers.py           # Funciones auxiliares
│
├── models/                  # Modelos Pydantic
│   ├── __init__.py
│   ├── acta.py
│   └── migracion.py
│
├── routers/                 # Rutas de la API
│   ├── __init__.py
│   ├── migracion.py
│   ├── pdf.py
│   └── health.py
│
├── logs/                    # Archivos de log
│   ├── migration.log
│   ├── pdf_generation.log
│   └── api. log
│
└── output/                  # Salida de PDFs generados
    └── pdfs/
        └── 2025-12-27/
```

---

## 🌐 API Endpoints

### Documentación Interactiva

Una vez iniciado el servidor, accede a:
- **Swagger UI**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **ReDoc**: [http://localhost:8000/redoc](http://localhost:8000/redoc)

### Endpoints Principales

#### Health Check
```http
GET /health
```
Verifica el estado del servicio. 

#### Migración de Datos
```http
POST /api/migracion/ejecutar
Content-Type: application/json

{
  "fecha_inicio": "2025-01-01",
  "fecha_fin": "2025-12-31",
  "tipo_documento": "traslado"
}
```

```http
GET /api/migracion/status/{migracion_id}
```

#### Generación de PDFs
```http
GET /api/pdf/generar/{acta_id}
```
Genera y descarga un PDF de acta específica.

```http
POST /api/pdf/batch
Content-Type: application/json

{
  "acta_ids": [1, 2, 3, 4, 5]
}
```

#### Listado de Actas
```http
GET /api/actas? tipo=traslado&fecha_desde=2025-01-01&fecha_hasta=2025-12-27
```

---

## 📊 Logging y Monitoreo

### Archivos de Log

Los logs se guardan en la carpeta `logs/` con rotación automática:

- `logs/migration.log` - Operaciones de migración
- `logs/pdf_generation.log` - Generación de PDFs
- `logs/api.log` - Peticiones y respuestas de API
- `logs/error.log` - Errores del sistema

### Niveles de Log

```python
DEBUG    # Información detallada para debugging
INFO     # Operaciones normales del sistema
WARNING  # Advertencias (no críticas)
ERROR    # Errores que afectan funcionalidad
CRITICAL # Errores críticos del sistema
```

### Consultar Logs

```bash
# Ver logs en tiempo real
tail -f logs/api.log

# Buscar errores
grep "ERROR" logs/migration.log
```

---

## 🚢 Despliegue

### Con Gunicorn (Producción)

```bash
pip install gunicorn

gunicorn main:app \
    --workers 4 \
    --worker-class uvicorn.workers. UvicornWorker \
    --bind 0.0.0.0:8000 \
    --access-logfile logs/access.log \
    --error-logfile logs/error.log
```

### Con Systemd (Linux)

Crea `/etc/systemd/system/xamp-backend.service`:

```ini
[Unit]
Description=Xamp Backend Service
After=network.target

[Service]
Type=notify
User=www-data
WorkingDirectory=/var/www/xamp_backend
Environment="PATH=/var/www/xamp_backend/env/bin"
ExecStart=/var/www/xamp_backend/env/bin/gunicorn main:app \
    --workers 4 \
    --worker-class uvicorn.workers.UvicornWorker \
    --bind 0.0.0.0:8000

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable xamp-backend
sudo systemctl start xamp-backend
sudo systemctl status xamp-backend
```

### Configuración Nginx

```nginx
server {
    listen 80;
    server_name api.tu-dominio.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

---

## 🔍 Troubleshooting

### Error: "WeasyPrint cannot find cairo"

**Solución:** Instala las dependencias del sistema (ver [Requisitos Previos](#requisitos-previos))

### Error: "Cannot connect to SQL Server"

**Solución:** 
- Verifica las credenciales en `.env`
- Confirma que el driver ODBC esté instalado
- Revisa la conectividad de red al servidor

### Error: "Permission denied" al generar PDFs

**Solución:**
```bash
mkdir -p output/pdfs logs
chmod 755 output logs
```

### Los PDFs se ven mal / sin estilos

**Solución:**
- Verifica que las rutas de CSS en las plantillas sean correctas
- Usa rutas absolutas o base64 para imágenes embebidas

---

## 📝 Notas Adicionales

- **Seguridad**: En producción, no expongas el puerto directamente.  Usa un reverse proxy (Nginx/Apache)
- **Backups**: Los PDFs se organizan por fecha. Implementa backups periódicos de la carpeta `output/`
- **Conexiones**: Las conexiones a bases de datos usan pool de conexiones para mejor rendimiento
- **Escalabilidad**: Para alto tráfico, considera usar Celery para procesar migraciones y generación de PDFs en background
- **Monitoreo**:  Integra herramientas como Sentry, New Relic o Prometheus para monitoreo en producción

---

## 🔐 Seguridad

- Nunca versiones el archivo `.env`
- Usa variables de entorno para credenciales sensibles
- Implementa rate limiting para prevenir abusos
- Actualiza dependencias regularmente:  `pip list --outdated`

---

## 📄 Licencia

Este proyecto es privado.  Todos los derechos reservados. 

---

## 👤 Autor

**DevSandroCobe**
- GitHub: [@DevSandroCobe](https://github.com/DevSandroCobe)

---

## 🤝 Soporte

¿Problemas, bugs o sugerencias? 
- Abre un [issue](https://github.com/DevSandroCobe/Xamp_backend/issues)
- Revisa la [documentación de la API](http://localhost:8000/docs)
- Consulta los logs en `logs/`

---

**¿Listo para integrar con el frontend?** Revisa el [Frontend Repository](https://github.com/DevSandroCobe/Xamp_frontend)
```
