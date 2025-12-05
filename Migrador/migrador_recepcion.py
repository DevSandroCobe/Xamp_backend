import logging
import sys
import os
from datetime import datetime
from Conexion.conexion_hana import ConexionHANA
from Conexion.conexion_sql import ConexionSQL
from Procesamiento.Importador import Importador
from Procesamiento.Importador_recepcion import ImportadorRecepcion
from Config.conexion_config import CONFIG_HANA
from pydantic import BaseModel

# ==========================================
# CONFIGURACION DE LOGS
# ==========================================
LOG_DIR = "Logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, 'migrador_recepcion.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class MigracionRecepcionRequest(BaseModel):
    fecha: datetime


class MigradorRecepcion:
    def __init__(self, fecha: datetime, almacen_id: str):
        # Asegurar que fecha sea datetime
        if isinstance(fecha, str):
            self.fecha = datetime.strptime(fecha, "%Y-%m-%d")
        else:
            self.fecha = fecha
            
        self.almacen_id = almacen_id
        self.importador = Importador()
        # Tablas objetivo para recepcion
        self.tablas_objetivo = ['RECEPCION', 'OWHS']
        self.queries = self._construir_queries()

    def _esquema(self, tabla):
        return CONFIG_HANA["schema"]

    def _formato_fecha_hana(self, columna):
        return f"TO_VARCHAR({columna}, 'YYYY-MM-DD')"

    def _construir_queries(self):
        fecha_str = self.fecha.strftime('%Y-%m-%d')
        
        # QUERY PRINCIPAL RECEPCION
        consulta_recepcion = f"""
        SELECT  
          OWTR."DocEntry", OWTR."DocNum", OWTR."DocDate", OWTR."Filler", OWTR."ToWhsCode", OWTR."U_SYP_MDTD", OWTR."U_SYP_MDSD", 
          OWTR."U_SYP_MDCD", OWTR."ObjType", OWTR."CardName", OWTR."U_BPP_FECINITRA",
          WTR1."DocEntry", WTR1."LineNum", WTR1."ItemCode", WTR1."Dscription", WTR1."WhsCode", WTR1."ObjType",
          OITL."LogEntry", OITL."ItemCode", OITL."DocEntry", OITL."DocLine", OITL."DocType", OITL."StockEff", OITL."LocCode",
          ITL1."LogEntry", ITL1."ItemCode", ITL1."Quantity", ITL1."SysNumber", ITL1."MdAbsEntry",
          OBTN."ItemCode", OBTN."DistNumber", OBTN."SysNumber", OBTN."AbsEntry", OBTN."MnfSerial", OBTN."ExpDate",
          OBTW."ItemCode", OBTW."MdAbsEntry", OBTW."WhsCode", OBTW."Location", OBTW."AbsEntry",
          OITM."ItemCode", OITM."ItemName", OITM."FrgnName", OITM."U_SYP_CONCENTRACION", OITM."U_SYP_FORPR", OITM."U_SYP_FFDET", 
          OITM."U_SYP_FABRICANTE"
        FROM
          {self._esquema("OWTR")}.OWTR OWTR
        INNER JOIN {self._esquema("WTR1")}.WTR1 WTR1 ON WTR1."DocEntry" = OWTR."DocEntry"
        LEFT JOIN {self._esquema("OITL")}.OITL OITL ON OITL."DocEntry" = OWTR."DocEntry"
                                         AND OITL."DocType" = OWTR."ObjType"
                                         AND OITL."DocLine" = WTR1."LineNum"
                                         AND OITL."ItemCode" = WTR1."ItemCode"
        LEFT JOIN {self._esquema("ITL1")}.ITL1 ITL1 ON ITL1."LogEntry" = OITL."LogEntry"
                                         AND ITL1."ItemCode" = WTR1."ItemCode"
        LEFT JOIN {self._esquema("OBTN")}.OBTN OBTN ON OBTN."SysNumber" = ITL1."SysNumber"
                                         AND OBTN."ItemCode" = WTR1."ItemCode"
        LEFT JOIN {self._esquema("OBTW")}.OBTW OBTW ON OBTW."ItemCode" = WTR1."ItemCode"
                                         AND OBTW."MdAbsEntry" = ITL1."MdAbsEntry"
                                         AND OBTW."WhsCode" = WTR1."WhsCode"
        LEFT JOIN {self._esquema("OITM")}.OITM OITM ON OITM."ItemCode" = WTR1."ItemCode"
        WHERE
          {self._formato_fecha_hana('OWTR."U_BPP_FECINITRA"')} = '{fecha_str}'
          AND OWTR."ToWhsCode" = '{self.almacen_id}' 
          AND OWTR."CANCELED" = 'N'
          AND OWTR."U_SYP_STATUS" = 'V'
          AND OWTR."U_SYP_MDSD" IS NOT NULL
          AND OWTR."U_SYP_MDCD" IS NOT NULL  
        """
        
        consulta_owhs = f"""
            SELECT T0."WhsCode", T0."WhsName", T0."TaxOffice"
            FROM {self._esquema("OWHS")}.OWHS T0
        """
        return {
            'RECEPCION': consulta_recepcion,
            'OWHS': consulta_owhs
        }

    def migracion_hana_sql(self, query: str, tabla_sql: str) -> int:
        logger.info(f"Migrando tabla objetivo: {tabla_sql}...")
        try:
            # 1. Extraccion HANA
            with ConexionHANA(query) as hana:
                if not hana.db_estado:
                    logger.error("Conexion a SAP HANA fallida")
                    return 0
                registros = hana.obtener_tabla()
                total = len(registros)
                logger.info(f"Registros extraidos de HANA para {tabla_sql}: {total}")
                
                if not registros:
                    logger.warning(f"No hay registros en HANA para {tabla_sql}")
                    return 0
                
                # 2. Procesamiento
                if tabla_sql == 'RECEPCION':
                    # Logica especifica RECEPCION
                    importador = ImportadorRecepcion()
                    for i, fila in enumerate(registros, 1):
                        importador.procesar_fila(fila)
                        if i % 500 == 0:
                            logger.info(f"Procesados en memoria {i} registros...")
                    
                    # Tablas destino SQL
                    tablas = ['OWTR', 'WTR1', 'OITL', 'ITL1', 'OBTN', 'OBTW', 'OITM']
                    
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logger.error("Conexion a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        
                        for t in tablas:
                            # A. Truncar
                            try:
                                cursor.execute(f"TRUNCATE TABLE dbo.{t}")
                                logger.info(f"Tabla dbo.{t} truncada correctamente.")
                            except Exception as e:
                                logger.critical(f"No se pudo truncar dbo.{t}: {e}")
                                return 0
                            
                            # B. Insertar
                            bloques = importador.obtener_bloques(t)
                            logger.info(f"Insertando {len(bloques)} bloques en {t}...")
                            
                            for j, bloque in enumerate(bloques, 1):
                                if not bloque.strip(): continue
                                try:
                                    cursor.execute(bloque)
                                except Exception as e:
                                    logger.error(f"Error insertando bloque {j} en {t}: {e}")
                            
                            # C. Commit
                            try:
                                sql.conexion.commit()
                                logger.info(f"Commit realizado para tabla {t}")
                            except Exception as e:
                                logger.critical(f"Error en COMMIT tabla {t}: {e}")
                                return 0
                        
                        logger.info(f"Proceso RECEPCION finalizado. Registros origen: {total}")
                        return total

                else:
                    # Logica Generica (OWHS)
                    self.importador = Importador()
                    for i, fila in enumerate(registros, 1):
                        self.importador.query_transaccion(fila, tabla_sql)
                    
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logger.error("Conexion a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        
                        # A. Truncar
                        try:
                            cursor.execute(f"TRUNCATE TABLE dbo.{tabla_sql}")
                            logger.info(f"Tabla dbo.{tabla_sql} truncada correctamente.")
                        except Exception as e:
                            logger.warning(f"Error al truncar dbo.{tabla_sql}: {e}")
                        
                        # B. Insertar
                        bloques = self.importador.query_sql
                        for j, bloque in enumerate(bloques, 1):
                            if not bloque.strip(): continue
                            try:
                                cursor.execute(bloque)
                            except Exception as e:
                                logger.error(f"Error insertando bloque {j} en {tabla_sql}: {e}")
                        
                        # C. Commit
                        sql.conexion.commit()
                        logger.info(f"Insertados en SQL Server: {total} registros en {tabla_sql}")
                        return total

        except Exception as e:
            logger.critical(f"Error general migrando {tabla_sql}: {e}")
            return 0

    def migrar_todas(self) -> list:
        resultados = []
        for tabla in self.tablas_objetivo:
            cantidad = self.migracion_hana_sql(self.queries[tabla], tabla)
            resultados.append({
                "tabla": tabla,
                "fecha": self.fecha.strftime("%Y-%m-%d"),
                "registros": cantidad,
                "exito": cantidad > 0 or (cantidad == 0)
            })
        return resultados