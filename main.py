
import logging
import time
import traceback
from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import date
import os
from fastapi.staticfiles import StaticFiles

# Módulos locales
from Migrador.migrador import Migrador
from Migrador.migrador_traslado import Migrador_traslado, MigracionTrasladoRequest
from Migrador.migrador_ventas import MigradorVentas
from generador_pdf.endpoints import acta_ventas, acta_traslado, acta_despacho, acta_recepcion, acta_organoleptico
from Migrador.migrado_despacho_1_y_5 import MigradorDespacho
from Migrador.migrador_recepcion import migrador_recepcion
from Migrador.migrador_organoleptico import MigradorOrganoleptico
# --------------------------------------------------------
# Configuración de Logging
# --------------------------------------------------------
LOG_FILE = "migracion.log"
logging.basicConfig(
    level=logging.INFO,  # Cambia a DEBUG si quieres más detalle
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()  # También imprime en consola
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------
# Inicialización de la app
# --------------------------------------------------------
app = FastAPI(title="API de Migración y Generación de PDFs")

# --------------------------------------------------------
# CORS
# --------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Montar carpeta de estáticos para acceso web (opcional pero recomendado)
app.mount("/static", StaticFiles(directory="generador_pdf/static"), name="static")

# --------------------------------------------------------
# Modelos de entrada
# --------------------------------------------------------
class MigracionRequest(BaseModel):
    fecha: date
    tabla: str = "*"

class MigracionTrasladoRequest(BaseModel):
    fecha: date
    almacen_id: str

class MigracionVentasRequest(BaseModel):
    fecha: date
    almacen_id: str

class MigracionDespachoRequest(BaseModel):
    fecha: date
    almacen_id: str

class MigracionOrganolepticoRequest(BaseModel):
    fecha: date
    almacen_id: str

class MigracionRecepcionRequest(BaseModel):
    fecha: date
    almacen_id: str

# --------------------------------------------------------
# Endpoints
# --------------------------------------------------------
@app.post("/")
def root():
    return {"mensaje": "✅ API Migrador funcionando correctamente"}

@app.post("/api/importar/")
async def importar_data(request: MigracionRequest = Body(...)):
    fecha_str = request.fecha.isoformat()
    logger.info(f"📅 Solicitud de migración para la fecha: {fecha_str}, tabla: {request.tabla}")

    try:
        migrador = Migrador(fecha_str=fecha_str)

        tablas = (
            ["OITM", "OWHS", "OWTR", "WTR1", "OITL", "ODLN",
             "OINV", "OBTW", "OBTN", "ITL1", "DLN1", "INV1", "IBT1"]
            if request.tabla == "*" else [request.tabla]
        )

        resultados = {}
        for tabla in tablas:
            logger.info(f"▶ Iniciando migración de tabla {tabla}")
            start_time = time.perf_counter()
            try:
                resultado = migrador.migrar_tabla(tabla)
                duracion = round(time.perf_counter() - start_time, 2)
                logger.info(f"✅ Tabla {tabla} migrada en {duracion} segundos")
                resultados[tabla] = {"status": "ok", "mensaje": resultado, "tiempo": duracion}
            except Exception as e:
                duracion = round(time.perf_counter() - start_time, 2)
                error_trace = traceback.format_exc()
                logger.error(f"❌ Error en tabla {tabla} ({duracion}s): {e}\n{error_trace}")
                resultados[tabla] = {"status": "error", "mensaje": str(e), "tiempo": duracion}

        logger.info("🏁 Migración completada")
        return {"status": "success", "fecha": fecha_str, "resultados": resultados}

    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(f"🔥 Error inesperado en importar_data: {e}\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"❌ Error inesperado: {str(e)}")

@app.post("/api/importar_traslados/")
async def importar_traslados(request: MigracionTrasladoRequest = Body(...)):
    fecha_str = request.fecha.strftime('%Y-%m-%d')
    logger.info(f"📅 Solicitud de migración de traslados para la fecha: {fecha_str}")
    try:
        migrador = Migrador_traslado(request.fecha)
        resultados = migrador.migrar_todas()
        logger.info("🏁 Migración de traslados completada")
        return {"status": "success", "fecha": fecha_str, "resultados": resultados}
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(f"🔥 Error inesperado en importar_traslados: {e}\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"❌ Error inesperado: {str(e)}")

@app.post("/api/importar_ventas/")
async def importar_ventas(request: MigracionVentasRequest = Body(...)):
    fecha_str = request.fecha.strftime('%Y-%m-%d')
    logger.info(f"📅 Solicitud de migración de ventas para la fecha: {fecha_str}")
    try:
        migrador = MigradorVentas(request.fecha)
        resultados = migrador.migrar_todas()
        logger.info("🏁 Migración de ventas completada")
        return {"status": "success", "fecha": fecha_str, "resultados": resultados}
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(f"🔥 Error inesperado en importar_ventas: {e}\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"❌ Error inesperado: {str(e)}")


# Endpoint para importar despacho
@app.post("/api/importar_despacho/")
async def importar_despacho(request: MigracionDespachoRequest = Body(...)):
    fecha_str = request.fecha.strftime('%Y-%m-%d')
    logger.info(f"📅 Solicitud de migración de despacho para la fecha: {fecha_str}")
    try:  
        migrador = MigradorDespacho(request.fecha)
        resultados = migrador.migrar_todas()
        logger.info("🏁 Migración de despacho completada")
        return {"status": "success", "fecha": fecha_str, "resultados": resultados}
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(f"🔥 Error inesperado en importar_despacho: {e}\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"❌ Error inesperado: {str(e)}")

# Endpoint para importar organoléptico
@app.post("/api/importar_organoleptico/")
async def importar_organoleptico(request: MigracionOrganolepticoRequest = Body(...)):
    fecha_str = request.fecha.strftime('%Y-%m-%d')
    logger.info(f"📅 Solicitud de migración organoléptica para la fecha: {fecha_str}")
    try:
        
        migrador = MigradorOrganoleptico(request.fecha)
        resultados = migrador.migrar_todas()
        logger.info("🏁 Migración organoléptica completada")
        return {"status": "success", "fecha": fecha_str, "resultados": resultados}
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(f"🔥 Error inesperado en importar_organoleptico: {e}\n{error_trace}")
        raise HTTPException(status_code=500, detail=f"❌ Error inesperado: {str(e)}")

# Endpoint para importar recepción
@app.post("/api/importar_recepcion/")
async def importar_recepcion(request: MigracionRecepcionRequest = Body(...)):
    fecha_str = request.fecha.strftime('%Y-%m-%d')
    logger.info(
        f"📅 Migración de recepción | Fecha: {fecha_str} | Almacén: {request.almacen_id}"
    )
    try:
        # 👉 pásale el almacen_id al migrador
        migrador = migrador_recepcion(request.fecha, request.almacen_id)
        resultados = migrador.migrar_todas()
        logger.info("🏁 Migración de recepción completada")
        return {
            "status": "success",
            "fecha": fecha_str,
            "almacen": request.almacen_id,
            "resultados": resultados
        }
    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(
            f"🔥 Error inesperado en importar_recepcion: {e}\n{error_trace}"
        )
        raise HTTPException(status_code=500, detail=f"❌ Error inesperado: {str(e)}")

# --------------------------------------------------------
# Routers adicionales
# --------------------------------------------------------

# Routers para PDFs de actas
app.include_router(acta_ventas.router, prefix="/api")
app.include_router(acta_traslado.router, prefix="/api")
app.include_router(acta_despacho.router, prefix="/api")
app.include_router(acta_recepcion.router, prefix="/api")
app.include_router(acta_organoleptico.router, prefix="/api")

