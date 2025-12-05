import logging
import sys
import os
from datetime import datetime, timedelta
from Conexion.conexion_hana import ConexionHANA
from Conexion.conexion_sql import ConexionSQL
from Procesamiento.Importador import Importador
from Procesamiento.importador_ventas import ImportadorVentas
from Config.conexion_config import CONFIG_HANA
from pydantic import BaseModel

# ==========================================
# CONFIGURACION DE LOGS
# ==========================================
LOG_DIR = "Logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_FILE = os.path.join(LOG_DIR, 'migrador_ventas.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class MigracionVentasRequest(BaseModel):
    fecha: datetime
    almacen_id: str = "*"


class MigradorVentas:
    def __init__(self, fecha: datetime, almacen_id: str):
        # Asegurar que fecha sea datetime
        if isinstance(fecha, str):
            self.fecha = datetime.strptime(fecha, "%Y-%m-%d")
        else:
            self.fecha = fecha
            
        self.importador = Importador()
        self.almacen_id = almacen_id
        # Tablas que este migrador va a procesar
        self.tablas_objetivo = ['VENTAS', 'OINV', 'INV1', 'OWHS']
        self.queries = self._construir_queries()

    def _esquema(self, tabla):
        return CONFIG_HANA["schema"]

    def _formato_fecha_hana(self, columna):
        return f"TO_VARCHAR({columna}, 'YYYY-MM-DD')"

    def _construir_queries(self):
        fecha_str = self.fecha.strftime('%Y-%m-%d')
        # Rango ampliado para OINV (Facturas) por si la fecha contable difiere ligeramente
        fecha_inicio = (self.fecha - timedelta(days=7)).strftime('%Y-%m-%d')
        fecha_fin = (self.fecha + timedelta(days=7)).strftime('%Y-%m-%d')

        # Filtro opcional por almacén
        condicion_almacen = ""
        if self.almacen_id != "*":
            condicion_almacen = f"AND ODLN.\"U_COB_LUGAREN\" = '{self.almacen_id}'"

        # 1. QUERY PRINCIPAL DE VENTAS (ODLN y Relacionadas)
        consulta_ventas = f"""
            SELECT
                ODLN."DocEntry",ODLN."ObjType",ODLN."DocNum",ODLN."CardCode",ODLN."CardName",ODLN."NumAtCard",ODLN."DocDate",
                ODLN."TaxDate",ODLN."U_SYP_MDTD",ODLN."U_SYP_MDSD",ODLN."U_SYP_MDCD",ODLN."U_COB_LUGAREN",ODLN."U_BPP_FECINITRA",
                DLN1."DocEntry",DLN1."ObjType",DLN1."WhsCode",DLN1."ItemCode",DLN1."LineNum",DLN1."Dscription",DLN1."UomCode",
                IBT1."ItemCode",IBT1."BatchNum",IBT1."WhsCode",IBT1."BaseEntry",IBT1."BaseType",IBT1."BaseLinNum",IBT1."Quantity",
                OBTN."ItemCode", OBTN."DistNumber",OBTN."SysNumber",OBTN."AbsEntry",OBTN."MnfSerial",OBTN."ExpDate",
                OBTW."ItemCode",OBTW."MdAbsEntry",OBTW."WhsCode",OBTW."Location",OBTW."AbsEntry",      
                OITL."LogEntry",OITL."ItemCode",OITL."DocEntry",OITL."DocLine",OITL."DocType",OITL."StockEff",OITL."LocCode",
                ITL1."LogEntry",ITL1."ItemCode",ITL1."Quantity",ITL1."SysNumber",ITL1."MdAbsEntry",
                OITM."ItemCode",OITM."ItemName",OITM."FrgnName",OITM."U_SYP_CONCENTRACION",OITM."U_SYP_FORPR",
                OITM."U_SYP_FFDET",OITM."U_SYP_FABRICANTE"
            FROM
                {self._esquema("ODLN")}.ODLN ODLN
            INNER JOIN {self._esquema("DLN1")}.DLN1 DLN1
                ON DLN1."DocEntry" = ODLN."DocEntry"
            INNER JOIN {self._esquema("IBT1")}.IBT1 IBT1
                ON IBT1."BaseEntry" = DLN1."DocEntry"
                AND IBT1."BaseType" = DLN1."ObjType"
                AND IBT1."WhsCode" = DLN1."WhsCode"
                AND IBT1."ItemCode" = DLN1."ItemCode"
                AND IBT1."BaseLinNum" = DLN1."LineNum"
            INNER JOIN {self._esquema("OBTN")}.OBTN OBTN
                ON OBTN."ItemCode" = DLN1."ItemCode"
                AND OBTN."DistNumber" = IBT1."BatchNum"
            INNER JOIN {self._esquema("OITL")}.OITL OITL
                ON OITL."DocEntry" = DLN1."DocEntry"
                AND OITL."ItemCode" = IBT1."ItemCode"
                AND OITL."DocType" = DLN1."ObjType"
                AND OITL."DocLine" = DLN1."LineNum"
                AND OITL."StockEff" = 1
            INNER JOIN {self._esquema("ITL1")}.ITL1 ITL1
                ON ITL1."LogEntry" = OITL."LogEntry"
                AND ITL1."SysNumber" = OBTN."SysNumber"
            INNER JOIN {self._esquema("OBTW")}.OBTW OBTW
                ON OBTW."ItemCode" = DLN1."ItemCode"
                AND OBTW."MdAbsEntry" = ITL1."MdAbsEntry"
                AND OBTW."WhsCode" = DLN1."WhsCode"
            INNER JOIN {self._esquema("OITM")}.OITM OITM
                ON OITM."ItemCode" = DLN1."ItemCode"
            WHERE
                {self._formato_fecha_hana('ODLN."U_BPP_FECINITRA"')} = '{fecha_str}'
                AND ODLN."CANCELED" = 'N'
                AND ODLN."U_SYP_STATUS" = 'V'
                AND ODLN."U_SYP_MDSD" IS NOT NULL
                AND ODLN."U_SYP_MDCD" IS NOT NULL
                {condicion_almacen}        
        """

        # 2. QUERY FACTURAS (OINV)
        consulta_oinv = f"""
            SELECT T0."DocEntry", T0."NumAtCard", T0."U_SYP_NGUIA", T0."ObjType", T0."DocNum", T0."CardCode", T0."CardName", T0."DocDate",
                T0."TaxDate", T0."U_SYP_MDTD", T0."U_SYP_MDSD", T0."U_SYP_MDCD", T0."U_COB_LUGAREN", T0."U_BPP_FECINITRA"
            FROM {self._esquema("OINV")}.OINV T0
            WHERE T0."CANCELED" = 'N'
            AND T0."DocDate" BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        """

        # 3. QUERY DETALLE FACTURAS (INV1)
        consulta_inv1 = f'''
            SELECT T0."DocEntry", T0."ObjType", T0."WhsCode", T0."ItemCode", T0."LineNum", T0."Dscription",
                   T0."UomCode", T0."BaseType", T0."BaseEntry"
            FROM {self._esquema("INV1")}.INV1 T0
            INNER JOIN {self._esquema("OINV")}.OINV T1 ON T0."DocEntry" = T1."DocEntry"
            WHERE T1."CANCELED" = 'N'
                AND T1."DocDate" = '{fecha_str}'
        '''

        # 4. QUERY ALMACENES (OWHS)
        consulta_owhs = f"""
            SELECT T0."WhsCode", T0."WhsName", T0."TaxOffice"
            FROM {self._esquema("OWHS")}.OWHS T0
        """

        return {
            'VENTAS': consulta_ventas,
            'OINV': consulta_oinv,
            'INV1': consulta_inv1,
            'OWHS': consulta_owhs,
        }

    def migracion_hana_sql(self, query: str, tabla_sql: str) -> int:
        logger.info(f"Migrando tabla objetivo: {tabla_sql}...")
        try:
            # 1. Obtener datos de HANA
            with ConexionHANA(query) as hana:
                if not hana.db_estado:
                    logger.error("Conexión a SAP HANA fallida")
                    return 0
                registros = hana.obtener_tabla()
                total = len(registros)
                logger.info(f"Registros extraídos de HANA para {tabla_sql}: {total}")
                
                if not registros:
                    logger.warning(f"No hay registros en HANA para {tabla_sql}")
                    return 0
                
                # 2. Procesar datos (Caso especial VENTAS vs Tablas normales)
                if tabla_sql == 'VENTAS':
                    # Lógica compleja: VENTAS se divide en ODLN, DLN1, etc.
                    importador = ImportadorVentas()
                    for i, fila in enumerate(registros, 1):
                        importador.procesar_fila(fila)
                        if i % 500 == 0:
                            logger.info(f"Procesados en memoria {i} registros...")
                    
                    # Lista de tablas SQL que llena el ImportadorVentas
                    tablas_destino = ['ODLN', 'DLN1', 'IBT1', 'ITL1', 'OBTN', 'OBTW', 'OITL', 'OITM']
                    
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logger.error("Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        
                        for t in tablas_destino:
                            # A. Truncar tabla
                            try:
                                cursor.execute(f"TRUNCATE TABLE dbo.{t}")
                                logger.info(f"Tabla dbo.{t} truncada correctamente.")
                            except Exception as e:
                                logger.critical(f"No se pudo truncar dbo.{t}: {e}")
                                return 0
                            
                            # B. Insertar bloques
                            bloques = importador.obtener_bloques(t)
                            logger.info(f"Insertando {len(bloques)} bloques en {t}...")
                            
                            for j, bloque in enumerate(bloques, 1):
                                if not bloque.strip(): continue
                                try:
                                    cursor.execute(bloque)
                                except Exception as e:
                                    logger.error(f"Error insertando bloque {j} en {t}: {e}")
                                    # logger.debug(f"SQL Fallido: {bloque[:100]}...")
                            
                            # C. Commit por tabla
                            try:
                                sql.conexion.commit()
                                logger.info(f"Commit realizado para tabla {t}")
                            except Exception as e:
                                logger.critical(f"Error en COMMIT tabla {t}: {e}")
                                return 0
                        
                        logger.info(f"Proceso VENTAS finalizado. Registros origen: {total}")
                        return total

                else:
                    # Lógica simple: OINV, INV1, OWHS (Mapeo directo 1 a 1)
                    self.importador = Importador() # Reiniciar importador genérico
                    
                    # Generar inserts en memoria
                    for i, fila in enumerate(registros, 1):
                        self.importador.query_transaccion(fila, tabla_sql)
                    
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logger.error("Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        
                        # A. Truncar
                        try:
                            cursor.execute(f"TRUNCATE TABLE dbo.{tabla_sql}")
                            logger.info(f"Tabla dbo.{tabla_sql} truncada correctamente.")
                        except Exception as e:
                            logger.warning(f"Error al truncar dbo.{tabla_sql} (posiblemente no exista o FK): {e}")
                        
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
        """
        Ejecuta la migración de todas las tablas definidas en self.tablas_objetivo
        """
        resultados = []
        for tabla in self.tablas_objetivo:
            cantidad = self.migracion_hana_sql(self.queries[tabla], tabla)
            resultados.append({
                "tabla": tabla,
                "fecha": self.fecha.strftime("%Y-%m-%d"),
                "registros": cantidad,
                "exito": cantidad > 0 or (cantidad == 0) # Consideramos exito si corrió sin excepción crítica
            })
        return resultados