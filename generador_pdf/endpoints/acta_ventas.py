import logging
import traceback
import os
import shutil
import time
import zipfile
from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel
from datetime import date, datetime
from Conexion.conexion_sql import ConexionSQL
from generador_pdf.pdf_generator import generar_pdf_acta_ventas

router = APIRouter()

# ==========================================
# CONFIGURACIÓN DE LOGS
# ==========================================
# 1. Definir carpeta y crearla si no existe en la raíz
LOG_DIR = "Logs"
os.makedirs(LOG_DIR, exist_ok=True)

# 2. Ruta completa del archivo log dentro de la carpeta
LOG_FILE = os.path.join(LOG_DIR, "pdf_ventas.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ],
)
logger = logging.getLogger(__name__)


class PDFVentasRequest(BaseModel):
    fecha: date
    firma: str


# --- FUNCIÓN DE LIMPIEZA ---
def borrar_archivos_temporales(ruta_carpeta):
    try:
        time.sleep(2)  # Espera un poco para asegurar que el archivo se ha enviado
        if os.path.exists(ruta_carpeta):
            shutil.rmtree(ruta_carpeta)
            logger.info(f"Carpeta temporal eliminada: {ruta_carpeta}")
    except Exception as e:
        logger.warning(f"No se pudo eliminar la carpeta {ruta_carpeta}: {e}")


# --- ENDPOINT PRINCIPAL ---
@router.post("/generar_pdf_ventas/")
def generar_pdf_ventas(data: PDFVentasRequest, background_tasks: BackgroundTasks):

    # 1. Preparar Carpetas Temporales para el ZIP
    timestamp = int(time.time())
    nombre_unico = f"Lote_Ventas_{timestamp}"
    
    # Crea una carpeta temporal base si no existe
    base_temp = os.path.join(os.getcwd(), "Temp_Downloads")
    
    carpeta_trabajo = os.path.join(base_temp, nombre_unico)
    carpeta_pdfs = os.path.join(carpeta_trabajo, "PDFs")

    # Creamos la estructura de carpetas
    os.makedirs(carpeta_pdfs, exist_ok=True)

    fecha_str_log = data.fecha.strftime("%Y-%m-%d")
    logger.info(f"Iniciando proceso masivo ZIP para fecha: {fecha_str_log}")

    try:
        # 2. LÓGICA SQL (Obtener DocEntries)
        with ConexionSQL() as conn:
            cursor = conn.cursor
            logger.info(f"Ejecutando SP: LISTADO_DOC_ENTREGA_VENTA '{fecha_str_log}'")
            cursor.execute("EXEC LISTADO_DOC_ENTREGA_VENTA ?", fecha_str_log)
            docentries = [row[0] for row in cursor.fetchall()]
            logger.info(f"DocEntries encontrados: {docentries}")

        if not docentries:
            raise HTTPException(
                status_code=404, detail="No se encontraron registros para esta fecha."
            )

        archivos_para_zip = []

        # 3. BUCLE DE GENERACIÓN
        for docentry in docentries:
            with ConexionSQL() as conn:
                cursor = conn.cursor
                logger.debug(f"Obteniendo datos para DocEntry {docentry}")
                cursor.execute("EXEC INFO_DOC_ENTREGA_VENTA ?", docentry)

                if cursor.description is None:
                    continue

                columnas = [col[0] for col in cursor.description]
                registros = [dict(zip(columnas, row)) for row in cursor.fetchall()]

            if not registros:
                logger.warning(f"DocEntry {docentry} no tiene registros. Saltando.")
                continue

            encabezado = registros[0]
            productos = registros[0:]

            # --- Formateo de productos ---
            productos_formateados = []
            for item in productos:
                def safe(val):
                    if val is None or str(val).strip().lower() == "none":
                        return ""
                    return str(val)

                try:
                    dia = safe(item.get("Dia"))
                    mes = safe(item.get("Mes"))
                    anio = item.get("Anio")
                    if anio is not None and anio != "":
                        anio = str(int(anio) + 2000)
                    else:
                        anio = ""
                    fecha_vcto = f"{dia.zfill(2)}/{mes.zfill(2)}/{anio}"
                except Exception:
                    fecha_vcto = "N/A"

                descripcion = " ".join([
                    safe(item.get("FrgnName")),
                    safe(item.get("Concentracion")),
                    safe(item.get("FormaFarmaceutica")),
                    safe(item.get("FormaPresentacion")),
                ]).strip()

                productos_formateados.append({
                    "cantidad": safe(item.get("Quantity", 0)),
                    "descripcion": descripcion,
                    "lote": safe(item.get("DistNumber")),
                    "vencimiento": fecha_vcto,
                    "rs": safe(item.get("MnfSerial")),
                    "condicion": safe(item.get("CondicionAlm")),
                })

            # --- Formateo de fechas ---
            fecha_str_pdf = data.fecha.strftime("%d-%m-%Y")
            docdate_val = encabezado.get("DocDate")
            if docdate_val:
                try:
                    if hasattr(docdate_val, "strftime"):
                        docdate_str = docdate_val.strftime("%d-%m-%Y")
                    else:
                        docdate_str = datetime.strptime(str(docdate_val)[:10], "%Y-%m-%d").strftime("%d-%m-%Y")
                except Exception:
                    docdate_str = str(docdate_val)
            else:
                docdate_str = ""

            # Obtener factura con fallbacks
            factura_num = (encabezado.get("NRO FACTURA") or encabezado.get("DocNum") or encabezado.get("FACTURA") or "")
            cliente_nom = encabezado.get("CardName") or ""
            
            data_pdf = {
                "fecha": fecha_str_pdf,
                "docdate": docdate_str,
                "factura": factura_num,
                "cliente": cliente_nom,
                "productos": productos_formateados,
                "firma": data.firma,
                "numCard": encabezado.get("NumAtCard"),
            }

            try:
                # Generamos el PDF
                ruta_original = generar_pdf_acta_ventas(data_pdf)

                # --- COPIAR AL TEMPORAL PARA ZIP ---
                if os.path.exists(ruta_original):
                    nombre_archivo = os.path.basename(ruta_original)
                    ruta_destino_zip = os.path.join(carpeta_pdfs, nombre_archivo)
                    shutil.copy2(ruta_original, ruta_destino_zip)
                    archivos_para_zip.append(ruta_destino_zip)
                    logger.info(f"PDF agregado al lote: {nombre_archivo}")
                else:
                    logger.error(f"El archivo generado no existe: {ruta_original}")

            except Exception as pdf_error:
                logger.error(f"Error generando PDF DocEntry {docentry}: {pdf_error}")
                logger.debug(traceback.format_exc())

        # 4. CREAR EL ZIP FINAL
        if not archivos_para_zip:
            raise HTTPException(
                status_code=500, detail="No se genero ningun PDF valido."
            )

        nombre_zip = f"Actas_Ventas_{fecha_str_log}.zip"
        ruta_zip_final = os.path.join(carpeta_trabajo, nombre_zip)

        logger.info(f"Comprimiendo {len(archivos_para_zip)} archivos...")
        with zipfile.ZipFile(ruta_zip_final, "w") as zipf:
            for archivo in archivos_para_zip:
                zipf.write(archivo, arcname=os.path.basename(archivo))

        # 5. PROGRAMAR LIMPIEZA Y ENVIAR
        background_tasks.add_task(borrar_archivos_temporales, carpeta_trabajo)

        logger.info("Enviando ZIP al cliente...")
        return FileResponse(
            path=ruta_zip_final, filename=nombre_zip, media_type="application/zip"
        )

    except Exception as e:
        borrar_archivos_temporales(carpeta_trabajo)
        logger.critical(f"Error critico: {e}")
        logger.debug(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# --- Endpoint para verificar migración ---
@router.get("/verificar_migracion/")
def verificar_migracion(fecha: str, grupo: str):
    logger.info(f"Verificando migracion: {fecha}, grupo: {grupo}")
    try:
        # Validación simple del grupo si es necesaria
        if grupo.lower() != "ventas":
             pass 

        with ConexionSQL() as conn:
            cursor = conn.cursor
            query = "SELECT COUNT(*) FROM ODLN WHERE CONVERT(date, U_BPP_FECINITRA) = ?"
            cursor.execute(query, fecha)
            count = cursor.fetchone()[0]
            migrado = count > 0
            
            mensaje = f"Datos {'ya migrados' if migrado else 'no migrados'} para {fecha}"
            logger.info(f"Resultado verificacion: {mensaje}")
            return {"migrado": migrado, "mensaje": mensaje}
            
    except Exception as e:
        logger.error(f"Error verificando: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")