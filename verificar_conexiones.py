import time
import json
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from Conexion.conexion_sql import ConexionSQL
from Conexion.conexion_hana import ConexionHANA
from Config.conexion_config import CONFIG_HANA
from tqdm import tqdm

# ------------------- CONFIGURACIÓN DE LOGGING -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ------------------- FUNCIONES -------------------

def test_sql():
    """Prueba de conexión a SQL Server"""
    inicio = time.perf_counter()
    sql = ConexionSQL()
    sql.conectar()

    if sql.db_estado:
        logging.info("SQL Server OK ✅")
    else:
        logging.error("SQL Server FAIL ❌")

    sql.cerrar_conexion()
    duracion = time.perf_counter() - inicio
    logging.info(f"⏱️ SQL Server check: {duracion:.3f}s\n")


def verificar_tabla(tabla, schema):
    """Verifica una tabla HANA, devuelve dict con resultados"""
    resultado = {
        "tabla": tabla,
        "exito": False,
        "sin_datos": False,
        "duracion": None,
        "error": None
    }

    query = f'SELECT TOP 1 * FROM {schema}."{tabla}"'
    t0 = time.perf_counter()

    try:
        with ConexionHANA(query) as hana:
            if hana.db_estado:
                registro = hana.obtener_registro()
                duracion = time.perf_counter() - t0
                resultado["duracion"] = duracion

                if registro:
                    resultado["exito"] = True
                    logging.info(f"✅ {tabla:<6} | {duracion:.3f}s | Datos OK")
                else:
                    resultado["exito"] = True
                    resultado["sin_datos"] = True
                    logging.warning(f"⚠️ {tabla:<6} | {duracion:.3f}s | Sin datos")
            else:
                resultado["error"] = "Error de conexión"
                logging.error(f"❌ {tabla:<6} | Error de conexión")
    except Exception as e:
        resultado["error"] = str(e)
        logging.error(f"❌ {tabla:<6} | EXCEPCIÓN: {e}")
        logging.debug(traceback.format_exc())

    return resultado


def test_hana(tablas=None, schema=None, max_workers=5):
    """Prueba de conexión y datos en HANA"""
    schema = schema or CONFIG_HANA.get("schema", "DEFAULT_SCHEMA")
    tablas = tablas or [
        'OITM', 'OBTW', 'OBTN', 'IBT1', 'OWHS', 'OINV',
        'INV1', 'OITL', 'ITL1', 'ODLN', 'DLN1', 'OWTR', 'WTR1'
    ]

    logging.info("🔎 Verificando tablas HANA...\n")
    resultados = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(verificar_tabla, tabla, schema): tabla for tabla in tablas}
        for f in tqdm(as_completed(futures), total=len(futures), desc="Tablas HANA"):
            res = f.result()
            resultados.append(res)

    # ------------------- RESUMEN -------------------
    exitos = [r for r in resultados if r["exito"]]
    errores = [r for r in resultados if r["error"]]
    sin_datos = [r for r in resultados if r.get("sin_datos")]

    duraciones = [r["duracion"] for r in exitos if r["duracion"] is not None]

    logging.info("\n📊 Resumen de performance")
    logging.info(f"   Total tablas medidas : {len(duraciones)}")
    if duraciones:
        logging.info(f"   Promedio por tabla   : {sum(duraciones)/len(duraciones):.3f}s")
        logging.info(f"   Más rápida           : {min(duraciones):.3f}s")
        logging.info(f"   Más lenta            : {max(duraciones):.3f}s")
    else:
        logging.warning("   No se pudo medir ninguna tabla.")

    logging.info(f"\n🟢 Tablas OK         : {len(exitos)}")
    if sin_datos:
        logging.warning(f"⚠️ Tablas sin datos : {len(sin_datos)} -> {[r['tabla'] for r in sin_datos]}")
    if errores:
        logging.error(f"🔴 Tablas con error : {len(errores)} -> {[r['tabla'] for r in errores]}")
    
    # ------------------- EXPORTAR RESULTADO -------------------
    with open("reporte_hana.json", "w") as f:
        json.dump(resultados, f, indent=2)
        logging.info("📄 Reporte exportado a 'reporte_hana.json'")


# ------------------- EJECUCIÓN -------------------
if __name__ == "__main__":
    test_sql()
    test_hana()
