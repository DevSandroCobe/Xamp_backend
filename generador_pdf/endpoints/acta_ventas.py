import logging
import traceback
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import date
from Conexion.conexion_sql import ConexionSQL
from generador_pdf.pdf_generator import generar_pdf_acta_ventas

router = APIRouter()

# Configuración de logging
LOG_FILE = "pdf_ventas.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PDFVentasRequest(BaseModel):
    fecha: date
    firma: str

@router.post("/generar_pdf_ventas/")
def generar_pdf_ventas(data: PDFVentasRequest):
    fecha = data.fecha.strftime("%Y-%m-%d")
    logger.info(f"➡ Fecha recibida: {fecha}")
    try:
        with ConexionSQL() as conn:
            cursor = conn.cursor
            logger.info(f"📦 Ejecutando SP: LISTADO_DOC_ENTREGA_VENTA '{fecha}'")
            cursor.execute("EXEC LISTADO_DOC_ENTREGA_VENTA ?", fecha)
            docentries = [row[0] for row in cursor.fetchall()]
            logger.info(f"➡ DocEntries encontrados: {docentries}")

        rutas_generadas = []

        for docentry in docentries:
            with ConexionSQL() as conn:
                cursor = conn.cursor
                logger.info(f"🔍 Obteniendo datos para DocEntry {docentry}")
                cursor.execute("EXEC INFO_DOC_ENTREGA_VENTA ?", docentry)
                columnas = [col[0] for col in cursor.description]
                registros = [dict(zip(columnas, row)) for row in cursor.fetchall()]

            if not registros:
                logger.warning(f"⚠️ DocEntry {docentry} no tiene registros. Saltando.")
                continue

            encabezado = registros[0]
            productos = registros[0:]

            # Formatear productos para el PDF
            productos_formateados = []
            for item in productos:
                def safe(val):
                    if val is None or str(val).strip().lower() == 'none':
                        return ""
                    return str(val)
                try:
                    dia = safe(item.get('Dia'))
                    mes = safe(item.get('Mes'))
                    anio = item.get('Anio')
                    if anio is not None and anio != '':
                        anio = str(int(anio) + 2000)
                    else:
                        anio = ''
                    fecha_vcto = f"{dia.zfill(2)}/{mes.zfill(2)}/{anio}"
                except Exception:
                    fecha_vcto = "N/A"

                descripcion = " ".join([
                    safe(item.get('FrgnName')),
                    safe(item.get('Concentracion')),
                    safe(item.get('FormaFarmaceutica')),
                    safe(item.get('FormaPresentacion'))                  
                ]).strip()

                productos_formateados.append({
                    "cantidad": safe(item.get("Quantity", 0)),
                    "descripcion": descripcion,
                    "lote": safe(item.get("DistNumber")),
                    "vencimiento": fecha_vcto,
                    "rs": safe(item.get("MnfSerial")),
                    "condicion": safe(item.get("CondicionAlm")),
                })

            fecha_str = data.fecha.strftime("%d-%m-%Y")
            docdate_val = encabezado.get("DocDate")
            if docdate_val:
                try:
                    if hasattr(docdate_val, 'strftime'):
                        docdate_str = docdate_val.strftime("%d-%m-%Y")
                    else:
                        from datetime import datetime
                        docdate_str = datetime.strptime(str(docdate_val)[:10], "%Y-%m-%d").strftime("%d-%m-%Y")
                except Exception:
                    docdate_str = str(docdate_val)
            else:
                docdate_str = ""

            data_pdf = {
                "fecha": fecha_str,
                "docdate": docdate_str,
                "factura": encabezado.get("NRO FACTURA", ""),
                "cliente": encabezado.get("CardName", ""),
                "productos": productos_formateados,
                "firma": data.firma,
                "numCard": encabezado.get("NumAtCard")
            }

            try:
                ruta = generar_pdf_acta_ventas(data_pdf)
                logger.info(f"✅ PDF generado: {ruta}")
                rutas_generadas.append(ruta)
            except Exception as pdf_error:
                logger.error(f"❌ Error generando PDF para DocEntry {docentry}: {pdf_error}")
                logger.debug(traceback.format_exc())

        logger.info(f"🏁 Proceso finalizado. Total PDFs: {len(rutas_generadas)}")
        return {"estado": "ok", "archivos_generados": rutas_generadas}

    except Exception as e:
        logger.critical(f"🔥 Error en generar_pdf_ventas: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generando PDF ventas: {str(e)}")

# Endpoint para verificar migración de ventas
@router.get("/verificar_migracion/")
def verificar_migracion(fecha: str, grupo: str):
    logger.info(f"🔍 Verificando migración para fecha: {fecha}, grupo: {grupo}")
    try:
        if grupo.lower() != "ventas":
            return {"migrado": False, "mensaje": "Grupo no válido"}
        with ConexionSQL() as conn:
            cursor = conn.cursor
            query = """
            SELECT COUNT(*) 
            FROM ODLN 
            WHERE CONVERT(date, U_BPP_FECINITRA) = ?
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
