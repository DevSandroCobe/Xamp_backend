import logging
import sys

# Configuración de logging
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(message)s'
logging.basicConfig(
    level=logging.DEBUG,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('migrador_ventas.log', encoding='utf-8')
    ]
)
from datetime import datetime , timedelta
from Conexion.conexion_hana import ConexionHANA
from Conexion.conexion_sql import ConexionSQL
from Procesamiento.Importador import Importador
from Procesamiento.importador_ventas import ImportadorVentas
from Config.conexion_config import CONFIG_HANA
from pydantic import BaseModel


class MigracionVentasRequest(BaseModel):
    fecha: datetime


class MigradorVentas:
    def __init__(self, fecha: datetime, almacen_id: str):
        self.fecha = fecha if isinstance(fecha, datetime) else datetime.strptime(str(fecha), "%Y-%m-%d")
        self.importador = Importador()
        self.almacen_id = almacen_id
        self.tablas_objetivo = ['VENTAS', 'OINV', 'INV1', 'OWHS']
        self.queries = self._construir_queries()

    def _esquema(self, tabla):
        return CONFIG_HANA["schema"]

    def _formato_fecha_hana(self, columna):
        return f"TO_VARCHAR({columna}, 'YYYY-MM-DD')"

    def _construir_queries(self):
        esq = CONFIG_HANA["schema"]
        fecha_str = self.fecha.strftime('%Y-%m-%d')
        # Calcular rango de fechas ±7 días
        fecha_inicio = (self.fecha - timedelta(days=7)).strftime('%Y-%m-%d')
        fecha_fin = (self.fecha + timedelta(days=7)).strftime('%Y-%m-%d')

        # Consulta principal de entregas de venta (ODLN como tabla principal)
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
                AND T0."U_COB_LUGAREN" = '{self.almacen_id}' 
        """

        consulta_oinv = f"""
            SELECT T0."DocEntry", T0."NumAtCard", T0."U_SYP_NGUIA", T0."ObjType", T0."DocNum", T0."CardCode", T0."CardName", T0."DocDate",
                T0."TaxDate", T0."U_SYP_MDTD", T0."U_SYP_MDSD", T0."U_SYP_MDCD", T0."U_COB_LUGAREN", T0."U_BPP_FECINITRA"
            FROM {self._esquema("OINV")}.OINV T0
            WHERE T0."CANCELED" = 'N'
            AND T0."DocDate" BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
        """
        consulta_inv1 = f'''
            SELECT T0."DocEntry", T0."ObjType", T0."WhsCode", T0."ItemCode", T0."LineNum", T0."Dscription",
                   T0."UomCode", T0."BaseType", T0."BaseEntry"
            FROM {self._esquema("INV1")}.INV1 T0
            INNER JOIN {self._esquema("OINV")}.OINV T1 ON T0."DocEntry" = T1."DocEntry"
            WHERE T1."CANCELED" = 'N'
                AND T1."DocDate" = '{fecha_str}'

        '''
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
        logging.info(f"🚀 Migrando {tabla_sql}...")
        try:
            with ConexionHANA(query) as hana:
                if not hana.db_estado:
                    logging.error("❌ Conexión a SAP HANA fallida")
                    return 0
                registros = hana.obtener_tabla()
                total = len(registros)
                logging.info(f"📥 Registros extraídos de HANA: {total}")
                if not registros:
                    logging.warning(f"⚠️ No hay registros en {tabla_sql}")
                    return 0
                if tabla_sql == 'VENTAS':
                    importador = ImportadorVentas()
                    for i, fila in enumerate(registros, 1):
                        importador.procesar_fila(fila)
                        if i % 100 == 0:
                            logging.info(f"📤 Procesados {i} registros...")
                    tablas = ['ODLN', 'DLN1', 'IBT1', 'ITL1', 'OBTN', 'OBTW', 'OITL', 'OITM']
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logging.error("❌ Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        for t in tablas:
                            try:
                                cursor.execute(f"TRUNCATE TABLE dbo.{t}")
                                logging.info(f"🧹 Tabla dbo.{t} truncada exitosamente.")
                            except Exception as e:
                                logging.warning(f"⚠️ Error al truncar dbo.{t}: {e}")
                            bloques = importador.obtener_bloques(t)
                            for j, bloque in enumerate(bloques, 1):
                                if not bloque.strip():
                                    continue
                                try:
                                    logging.debug(f"🛰️ Ejecutando bloque {j} de {t}...")
                                    cursor.execute(bloque)
                                except Exception as e:
                                    logging.error(f"❌ Error en bloque {j} ({t}): {e}")
                                    logging.error(f"🔎 Bloque problemático:\n{bloque}")
                        sql.conexion.commit()
                        logging.info(f"✅ Insertados en SQL Server: {total} registros de VENTAS")
                        return total
                else:
                    self.importador = Importador()
                    for i, fila in enumerate(registros, 1):
                        self.importador.query_transaccion(fila, tabla_sql)
                        if i % 100 == 0:
                            logging.info(f"📤 Procesados {i} registros...")
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logging.error("❌ Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor     
                        try:
                            cursor.execute(f"TRUNCATE TABLE dbo.{tabla_sql}")
                            logging.info(f"🧹 Tabla dbo.{tabla_sql} truncada exitosamente.")
                        except Exception as e:
                            logging.warning(f"⚠️ Error al truncar dbo.{tabla_sql}: {e}")
                        bloques = self.importador.query_sql
                        for j, bloque in enumerate(bloques, 1):
                            if not bloque.strip():
                                continue
                            try:
                                logging.debug(f"🛰️ Ejecutando bloque {j} de {tabla_sql}...")
                                cursor.execute(bloque)
                            except Exception as e:
                                logging.error(f"❌ Error en bloque {j} ({tabla_sql}): {e}")
                                logging.error(f"🔎 Bloque problemático:\n{bloque}")
                        sql.conexion.commit()
                        logging.info(f"✅ Insertados en SQL Server: {total} registros de {tabla_sql}")
                        return total
        except Exception as e:
            logging.critical(f"❌ Error migrando {tabla_sql}: {e}")
            return 0

    def migrar_todas(self) -> list:
        resultados = []
        for tabla in self.tablas_objetivo:
            cantidad = self.migracion_hana_sql(self.queries[tabla], tabla)
            resultados.append({
                "tabla": tabla,
                "fecha": self.fecha.strftime("%Y-%m-%d"),
                "registros": cantidad,
                "exito": cantidad > 0
            })
        return resultados
