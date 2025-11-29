from logger import logger
from datetime import datetime, timedelta
from Conexion.conexion_hana import ConexionHANA
from Conexion.conexion_sql import ConexionSQL
from Procesamiento.Importador import Importador
from Procesamiento.Importador_despacho import ImportadorDespacho
from Config.conexion_config import CONFIG_HANA
from pydantic import BaseModel


class MigracionDespachoRequest(BaseModel):
    fecha: datetime


class MigradorDespacho:
    def __init__(self, fecha: datetime,  almacen_id: str):
        self.fecha = fecha if isinstance(fecha, datetime) else datetime.strptime(str(fecha), "%Y-%m-%d")
        self.importador = Importador()
        self.almacen_id = almacen_id
        self.tablas_objetivo = ['DESPACHO', 'OWHS']  # SOLO ESTAS TABLAS
        self.queries = self._construir_queries()

    def _esquema(self, tabla):
        return CONFIG_HANA["schema"]

    def _formato_fecha_hana(self, columna):
        return f"TO_VARCHAR({columna}, 'YYYY-MM-DD')"

    def _construir_queries(self):
        fecha_inicio = (self.fecha - timedelta(days=2)).strftime('%Y-%m-%d')
        fecha_fin = (self.fecha + timedelta(days=2)).strftime('%Y-%m-%d')

        logger.debug(f"📅 Fecha objetivo: {self.fecha.strftime('%Y-%m-%d')}")
        logger.debug(f"📅 Rango usado: {fecha_inicio} → {fecha_fin}")

        consulta_despacho = f'''
           SELECT
          
            OINV."DocEntry", OINV."NumAtCard", OINV."U_SYP_NGUIA", OINV."ObjType", OINV."DocNum", 
            OINV."CardCode", OINV."CardName", OINV."DocDate", OINV."TaxDate", 
            OINV."U_SYP_MDTD", OINV."U_SYP_MDSD", OINV."U_SYP_MDCD", 
            OINV."U_COB_LUGAREN", OINV."U_BPP_FECINITRA",
            
        
            INV1."DocEntry", INV1."ObjType", INV1."WhsCode", INV1."ItemCode", 
            INV1."LineNum", INV1."Dscription", INV1."UomCode",
            INV1."BaseType", INV1."BaseEntry",

           
            IBT1."ItemCode", IBT1."BatchNum", IBT1."WhsCode", IBT1."BaseEntry", 
            IBT1."BaseType", IBT1."BaseLinNum", IBT1."Quantity",
            
          
            OBTN."ItemCode", OBTN."DistNumber", OBTN."SysNumber", OBTN."AbsEntry", 
            OBTN."MnfSerial", OBTN."ExpDate",

            
            OBTW."ItemCode", OBTW."MdAbsEntry", OBTW."WhsCode", OBTW."Location", OBTW."AbsEntry",
            
        
            OITL."LogEntry", OITL."ItemCode", OITL."DocEntry", OITL."DocLine", 
            OITL."DocType", OITL."StockEff", OITL."LocCode",

            ITL1."LogEntry", ITL1."ItemCode", ITL1."Quantity", ITL1."SysNumber", ITL1."MdAbsEntry",

           
            OITM."ItemCode", OITM."ItemName", OITM."FrgnName", 
            OITM."U_SYP_CONCENTRACION", OITM."U_SYP_FORPR", 
            OITM."U_SYP_FFDET", OITM."U_SYP_FABRICANTE"

        FROM {self._esquema("OINV")}.OINV OINV
            INNER JOIN {self._esquema("INV1")}.INV1 INV1 
                ON INV1."DocEntry" = OINV."DocEntry"
            LEFT JOIN {self._esquema("IBT1")}.IBT1 IBT1 
                ON IBT1."BaseEntry" = INV1."DocEntry" 
                AND IBT1."BaseType" = INV1."ObjType" 
                AND IBT1."WhsCode" = INV1."WhsCode" 
                AND IBT1."ItemCode" = INV1."ItemCode" 
                AND IBT1."BaseLinNum" = INV1."LineNum" 
                AND IBT1."Quantity" < 0
            LEFT JOIN {self._esquema("OBTN")}.OBTN OBTN 
                ON OBTN."ItemCode" = INV1."ItemCode" 
                AND OBTN."DistNumber" = IBT1."BatchNum"
            LEFT JOIN {self._esquema("OITL")}.OITL OITL 
                ON OITL."DocEntry" = INV1."DocEntry" 
                AND OITL."ItemCode" = IBT1."ItemCode" 
                AND OITL."DocType" = INV1."ObjType" 
                AND OITL."DocLine" = INV1."LineNum" 
                AND OITL."StockEff" = 1
            LEFT JOIN {self._esquema("ITL1")}.ITL1 ITL1 
                ON ITL1."LogEntry" = OITL."LogEntry" 
                AND ITL1."Quantity" = IBT1."Quantity" 
                AND ITL1."SysNumber" = OBTN."SysNumber"
            LEFT JOIN {self._esquema("OBTW")}.OBTW OBTW 
                ON OBTW."ItemCode" = INV1."ItemCode"
                AND OBTW."MdAbsEntry" = ITL1."MdAbsEntry" 
                AND OBTW."WhsCode" = INV1."WhsCode"
            INNER JOIN {self._esquema("OITM")}.OITM OITM 
                ON OITM."ItemCode" = INV1."ItemCode"
            WHERE OINV."CANCELED" = 'N'
            AND OINV."DocDate" BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            AND OINV."U_COB_LUGAREN" = '{self.almacen_id}' 
        
        '''

        consulta_owhs = f"""
            SELECT T0."WhsCode", T0."WhsName", T0."TaxOffice"
            FROM {self._esquema("OWHS")}.OWHS T0
        """
        return {
            'DESPACHO': consulta_despacho,
            'OWHS': consulta_owhs,
        }

    def migracion_hana_sql(self, query: str, tabla_sql: str) -> int:
        logger.info(f"🚀 Iniciando migración de {tabla_sql}...")
        try:
            with ConexionHANA(query) as hana:
                if not hana.db_estado:
                    logger.error("❌ Conexión a SAP HANA fallida")
                    return 0

                registros = hana.obtener_tabla()
                total = len(registros)
                logger.info(f"📥 Extraídos {total} registros desde HANA ({tabla_sql})")

                if not registros:
                    logger.warning(f"⚠️ No se encontraron registros en {tabla_sql}")
                    return 0

                if tabla_sql == 'DESPACHO':
                    importador = ImportadorDespacho()
                    for i, fila in enumerate(registros, 1):
                        importador.procesar_fila(fila)
                        if i % 500 == 0:
                            logger.debug(f"📤 Procesados {i}/{total} registros de {tabla_sql}...")

                    tablas = ['OINV', 'INV1', 'IBT1', 'OBTN', 'OBTW', 'OITL', 'ITL1', 'OITM']
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logger.error("❌ Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        for t in tablas:
                            try:
                                logger.debug(f"🧹 Truncando dbo.{t}...")
                                cursor.execute(f"TRUNCATE TABLE dbo.{t}")
                                logger.info(f"🧹 Tabla dbo.{t} truncada")
                            except Exception as e:
                                logger.warning(f"⚠️ No se pudo truncar dbo.{t}: {e}")
                            bloques = importador.obtener_bloques(t)
                            logger.debug(f"📦 Se generaron {len(bloques)} bloques para {t}")
                            for j, bloque in enumerate(bloques, 1):
                                if not bloque.strip():
                                    continue
                                try:
                                    cursor.execute(bloque)
                                except Exception as e:
                                    logger.error(f"❌ Error en bloque {j} de {t}: {e}")
                                    logger.error(f"🔎 SQL Problemático:\n{bloque}")
                        sql.conexion.commit()
                        logger.info(f"✅ {total} registros migrados de {tabla_sql} a SQL Server")
                        return total

                else:  # Caso OWHS
                    self.importador = Importador()
                    for fila in registros:
                        self.importador.query_transaccion(fila, tabla_sql)

                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            logger.error("❌ Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        try:
                            logger.debug(f"🧹 Truncando dbo.{tabla_sql}...")
                            cursor.execute(f"TRUNCATE TABLE dbo.{tabla_sql}")
                            logger.info(f"🧹 Tabla dbo.{tabla_sql} truncada")
                        except Exception as e:
                            logger.warning(f"⚠️ No se pudo truncar dbo.{tabla_sql}: {e}")

                        bloques = self.importador.query_sql
                        logger.debug(f"📦 Se generaron {len(bloques)} bloques para {tabla_sql}")
                        for j, bloque in enumerate(bloques, 1):
                            if not bloque.strip():
                                continue
                            try:
                                cursor.execute(bloque)
                            except Exception as e:
                                logger.error(f"❌ Error en bloque {j} de {tabla_sql}: {e}")
                                logger.error(f"🔎 SQL Problemático:\n{bloque}")
                        sql.conexion.commit()
                        logger.info(f"✅ {total} registros migrados de {tabla_sql} a SQL Server")
                        return total

        except Exception as e:
            logger.critical(f"💥 Error migrando {tabla_sql}: {e}", exc_info=True)
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
