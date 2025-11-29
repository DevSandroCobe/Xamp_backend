import logging
import traceback
from pydantic import BaseModel
from datetime import date
from fastapi import APIRouter, HTTPException
from Conexion.conexion_sql import ConexionSQL
from generador_pdf.pdf_generator import generar_pdf_acta_organoleptico

# Agrega esto junto con tus otros imports
from typing import Optional

# --------------------------------------------------------
# Configuración del Logger
# --------------------------------------------------------
LOG_FILE = "pdf_organoleptico.log"
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

class PDFOrganolepticoRequest(BaseModel):
    fecha: date
    firma: str


# --------------------------------------------------------
# Función utilitaria para limpiar el nombre de la firma
# --------------------------------------------------------
def limpiar_nombre_firma(firma: str) -> str:
    """
    Convierte 'FirmaAlfredoRoldanEsparraga.png' en 'Alfredo Roldan Esparraga'
    """
    if not firma:
        return ""
    nombre = firma.replace("Firma", "").replace(".png", "")
    # Insertar espacios antes de mayúsculas (excepto la primera letra)
    nombre = "".join([" " + c if c.isupper() and i != 0 else c for i, c in enumerate(nombre)])
    return nombre.strip()


@router.post("/generar_pdf_organoleptico/")
def generar_pdf_organoleptico(data: PDFOrganolepticoRequest):
    fecha = data.fecha.strftime("%Y-%m-%d")
    logger.info(f"➡ Fecha recibida: {fecha}")

    try:
        # 1️⃣ Obtener lista de DocEntries
        with ConexionSQL() as conn:
            cursor = conn.cursor
            logger.info(f"📦 Ejecutando SP: LISTADO_DOC_ORGA_TS '{fecha}'")
            cursor.execute("{CALL LISTADO_DOC_ORGA_TS (?)}", fecha)
            rows = cursor.fetchall()
            docentries = [row[0] for row in rows]
            logger.info(f"➡ DocEntries encontrados: {docentries}")

        rutas_generadas = []

        # 2️⃣ Procesar cada DocEntry
        for docentry in docentries:
            with ConexionSQL() as conn:
                cursor = conn.cursor
                logger.info(f"🔍 Obteniendo datos para DocEntry {docentry}")
                cursor.execute("EXEC INFO_DOC_ORGANO_LEP_TS ?", docentry)
                columnas = [col[0] for col in cursor.description]
                registros = [dict(zip(columnas, row)) for row in cursor.fetchall()]

            if not registros:
                logger.warning(f"⚠️ DocEntry {docentry} no tiene registros. Saltando.")
                continue

            encabezado = registros[0]
            productos = registros[0:]

            logger.info(f"📄 Generando PDF para DocEntry {docentry} | Guia: {encabezado.get('NroDeGuia')}")

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
                    safe(item.get('Nombre')).strip(),
                    safe(item.get('Concentracion')).strip(),
                    safe(item.get('FormaPresentacion')).strip(),
                    safe(item.get('FormaFarmaceutica')).strip()
                ]).strip()

                nombre_producto = " ".join([
                    safe(item.get('Nombre')).strip(),
                    safe(item.get('Concentracion')).strip(),
                    safe(item.get('FormaPresentacion')).strip()
                ]).strip()

                productos_formateados.append({
                    "cantidad": safe(item.get("Quantity", 0)),
                    "descripcion": descripcion,
                    "lote": safe(item.get("Lote")),
                    "vencimiento": fecha_vcto,
                    "rs": safe(item.get("RegistroSan")),
                    "condicion": safe(item.get("CondAlmac")),
                    "nombre_producto": nombre_producto,
                    "fabricante": safe(item.get("Fabricante")),
                    "presentacion": safe(item.get("FormaFarmaceutica", "")),
                })

            fecha_str = data.fecha.strftime("%d-%m-%Y")

            # Formatear DocDate (puede venir como datetime o string)
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

            # Tomar el primer producto para los campos individuales
            primer_producto = productos_formateados[0] if productos_formateados else {}

            # ✅ Procesar la firma
            firma_limpia = limpiar_nombre_firma(data.firma)

            data_pdf = {
                "fecha": fecha_str,
                "docdate": docdate_str,
                "guia": encabezado.get("NroDeGuia", ""),
                "almacen": encabezado.get("Almacen", ""),
                "descripcion": encabezado.get("NombreProd", ""),
                "nombre_producto": primer_producto.get("nombre_producto", ""),
                "cantidad": primer_producto.get("cantidad", ""),
                "lote": primer_producto.get("Lote", ""),
                "rs": primer_producto.get("RegistroSan", ""),
                "fabricante": primer_producto.get("fabricante", ""),
                "condicion": primer_producto.get("CondAlmac", ""),
                "fecha_vencimiento": primer_producto.get("vencimiento", ""),
                "presentacion": primer_producto.get("FormaFarmaceutica", ""),
                # Firma original (archivo PNG)
                "firma": data.firma,
                # Firma limpia (texto para mostrar)
                "nombre_firma": firma_limpia,
                "productos": productos_formateados,
            }

            try:
                ruta = generar_pdf_acta_organoleptico(data_pdf)
                logger.info(f"✅ PDF generado: {ruta}")
                rutas_generadas.append(ruta)
            except Exception as pdf_error:
                logger.error(f"❌ Error generando PDF para DocEntry {docentry}: {pdf_error}")
                logger.debug(traceback.format_exc())

        logger.info(f"🏁 Proceso finalizado. Total PDFs: {len(rutas_generadas)}")
        return {"estado": "ok", "archivos_generados": rutas_generadas}

    except Exception as e:
        logger.critical(f"🔥 Error en generar_pdf_organoleptico: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error generando PDF traslado: {str(e)}")


# --------------------------------------------------------
# Verificar Migración
# --------------------------------------------------------
class VerificarMigracionRequest(BaseModel):
    fecha: date
    grupo: str

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
