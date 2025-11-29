import logging
import traceback
from pydantic import BaseModel
from datetime import date
from fastapi import APIRouter, HTTPException
from Conexion.conexion_sql import ConexionSQL
from generador_pdf.pdf_generator import generar_pdf_acta_traslado

# Agrega esto junto con tus otros imports
from typing import Optional

# --------------------------------------------------------
# Configuración del Logger
# --------------------------------------------------------
LOG_FILE = "pdf_traslado.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------
# Router
# --------------------------------------------------------
router = APIRouter()

class PDFTrasladoRequest(BaseModel):
    fecha: date
    firma: str  # <-- ahora acepta firma

@router.post("/generar_pdf_traslado/")
def generar_pdf_traslado(data: PDFTrasladoRequest):
    fecha = data.fecha.strftime("%Y-%m-%d")
    logger.info(f"➡ Fecha recibida: {fecha}")

    try:
        # 1️⃣ Obtener lista de DocEntries
        with ConexionSQL() as conn:
            cursor = conn.cursor
            logger.info(f"📦 Ejecutando SP: LISTADO_DOC_DESPACHO_TRASLADOS '{fecha}'")
            cursor.execute("{CALL LISTADO_DOC_DESPACHO_TRASLADOS (?)}", fecha)
            rows = cursor.fetchall()
            docentries = [row[0] for row in rows]
            logger.info(f"➡ DocEntries encontrados: {docentries}")

        rutas_generadas = []

        # 2️⃣ Procesar cada DocEntry
        for docentry in docentries:
            with ConexionSQL() as conn:
                cursor = conn.cursor
                logger.info(f"🔍 Obteniendo datos para DocEntry {docentry}")
                cursor.execute("EXEC INFO_DOC_DESPACHO_TRASLADOS ?", docentry)
                columnas = [col[0] for col in cursor.description]
                registros = [dict(zip(columnas, row)) for row in cursor.fetchall()]

            if not registros:
                logger.warning(f"⚠️ DocEntry {docentry} no tiene registros. Saltando.")
                continue

            encabezado = registros[0]
            productos = registros[0:]

            logger.info(f"📄 Generando PDF para DocEntry {docentry} | Guia: {encabezado.get('NroGuia')}")

            # Formatear productos para el PDF
            productos_formateados = []
            for item in productos:
                # Convertir None a "" en todos los campos relevantes
                def safe(val):
                    if val is None or str(val).strip().lower() == 'none':
                        return ""
                    return str(val)
                try:
                    dia = safe(item.get('Dia'))
                    mes = safe(item.get('Mes'))
                    anio = item.get('Anio')
                    if anio is not None:
                        anio = str(int(anio) + 2000)
                    else:
                        anio = ''
                    fecha_vcto = f"{dia.zfill(2)}/{mes.zfill(2)}/{anio}"
                except Exception:
                    fecha_vcto = "N/A"

                descripcion = " ".join([
                    safe(item.get('NombreComercial')).strip(),
                    safe(item.get('Concentracion')).strip(),
                    safe(item.get('FormaFarmaceutica')).strip(),
                    safe(item.get('FormaPresentacion')).strip()
                ]).strip()

                productos_formateados.append({
                    "cantidad": safe(item.get("CantidadLote", 0)),
                    "descripcion": descripcion,
                    "lote": safe(item.get("NroLote")),
                    "vencimiento": fecha_vcto,
                    "rs": safe(item.get("RegistroSanit")),
                    "condicion": safe(item.get("CondicionAlm")),
                })

            fecha_str = data.fecha.strftime("%d-%m-%Y")

            # Formatear DocDate (puede venir como datetime o string)
            docdate_val = encabezado.get("DocDate")
            if docdate_val:
                try:
                    # Si es datetime, formatear; si es string, dejar igual
                    if hasattr(docdate_val, 'strftime'):
                        docdate_str = docdate_val.strftime("%d-%m-%Y")
                    else:
                        # Si viene como 'YYYY-MM-DD' o similar
                        from datetime import datetime
                        docdate_str = datetime.strptime(str(docdate_val)[:10], "%Y-%m-%d").strftime("%d-%m-%Y")
                except Exception:
                    docdate_str = str(docdate_val)
            else:
                docdate_str = ""

            data_pdf = {
                "fecha": fecha_str,
                "docdate": docdate_str,
                "guia": encabezado["NroGuia"],       
                "AlmOrigen": encabezado["AlmOrigen"],   
                "AlmDestino": encabezado["AlmDestino"], 
                "productos": productos_formateados,
                "firma": data.firma,  # <-- pasa la firma a la plantilla
            }

            try:
                ruta = generar_pdf_acta_traslado(data_pdf)
                logger.info(f"✅ PDF generado: {ruta}")
                rutas_generadas.append(ruta)
            except Exception as pdf_error:
                logger.error(f"❌ Error generando PDF para DocEntry {docentry}: {pdf_error}")
                logger.debug(traceback.format_exc())

        logger.info(f"🏁 Proceso finalizado. Total PDFs: {len(rutas_generadas)}")
        return {"estado": "ok", "archivos_generados": rutas_generadas}

    except Exception as e:
        logger.critical(f"🔥 Error en generar_pdf_traslado: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generando PDF traslado: {str(e)}")



# Agrega este modelo Pydantic
class VerificarMigracionRequest(BaseModel):
    fecha: date
    grupo: str

# Agrega este endpoint (junto con tus otros routers)
@router.get("/verificar_migracion/")
def verificar_migracion(fecha: str, grupo: str):
    """
    Verifica si los datos para una fecha y grupo específico ya fueron migrados
    """
    logger.info(f"🔍 Verificando migración para fecha: {fecha}, grupo: {grupo}")

    try:
        # 1️⃣ Validar parámetros
        if grupo.lower() != "traslados":
            return {"migrado": False, "mensaje": "Grupo no válido"}

        # 2️⃣ Consultar a la base de datos usando U_BPP_FECINITRA
        with ConexionSQL() as conn:
            cursor = conn.cursor
            # U_BPP_FECINITRA es tipo string (YYYY-MM-DD) o date, así que convertimos a string para comparar
            query = """
            SELECT COUNT(*) 
            FROM OWTR 
            WHERE CONVERT(varchar(10), U_BPP_FECINITRA, 120) = ?
            """
            cursor.execute(query, fecha)
            count = cursor.fetchone()[0]

            migrado = count > 0
            mensaje = f"Datos {'ya migrados' if migrado else 'no migrados'} para {fecha}"

            logger.info(f"✔ Resultado verificación: {mensaje}")
            return {
                "migrado": migrado,
                "mensaje": mensaje,
                "detalle": {
                    "fecha_consultada": fecha,
                    "registros_encontrados": count
                }
            }

    except Exception as e:
        logger.error(f"❌ Error verificando migración: {str(e)}")
        logger.debug(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=f"Error al verificar migración: {str(e)}"
        )