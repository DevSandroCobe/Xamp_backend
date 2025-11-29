from datetime import datetime
from Conexion.conexion_hana import ConexionHANA
from Conexion.conexion_sql import ConexionSQL
from Procesamiento.Importador import Importador
from Procesamiento.Importador_traslado import ImportadorTraslado
from Config.conexion_config import CONFIG_HANA
from pydantic import BaseModel
from typing import List


class MigracionTrasladoRequest(BaseModel):
    fecha: datetime


class Migrador_traslado:
    def __init__(self, fecha: datetime, almacen_id: str):
        self.fecha = fecha if isinstance(fecha, datetime) else datetime.strptime(str(fecha), "%Y-%m-%d")
        self.importador = Importador()
        self.almacen_id = almacen_id
        self.tablas_objetivo = ['TRASLADOS', 'OWHS']
        self.queries = self._construir_queries()

    def _esquema(self, tabla):
        return CONFIG_HANA["schema"]

    def _formato_fecha_hana(self, columna):
        return f"TO_VARCHAR({columna}, 'YYYY-MM-DD')"

    def _condicion_filler(self):
        if self.almacen_id == '15':
            # Cuando es 15, incluye 16 también
            return 'IN (\'15\', \'16\')'
        else:
            # Para cualquier otro ID, solo ese
            return f"= '{self.almacen_id}'"

    def _construir_queries(self):
        esq = CONFIG_HANA["schema"] 
        fecha_str = self.fecha.strftime('%Y-%m-%d')
        cond_filler = self._condicion_filler()
        consulta_traslados = f"""
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
          AND OWTR."CANCELED" = 'N'
          AND OWTR."U_SYP_STATUS" = 'V'
          AND OWTR."U_SYP_MDSD" IS NOT NULL
          AND OWTR."U_SYP_MDCD" IS NOT NULL
          AND OWTR."Filler" {cond_filler}
          AND OWTR."ToWhsCode" IN ('01', '09')
        ;
        """
        consulta_owhs = f"""
        SELECT T0."WhsCode", T0."WhsName", T0."TaxOffice"
        FROM {self._esquema("OWHS")}.OWHS T0
        """
        return {
            'TRASLADOS': consulta_traslados,
            'OWHS': consulta_owhs
        }

    def migracion_hana_sql(self, query: str, tabla_sql: str) -> int:
        print(f"\n🚀 Migrando {tabla_sql}...")
        try:
            with ConexionHANA(query) as hana:
                if not hana.db_estado:
                    print("❌ Conexión a SAP HANA fallida")
                    return 0
                registros = hana.obtener_tabla()
                total = len(registros)
                print(f"📥 Registros extraídos de HANA: {total}")
                if not registros:
                    print(f"⚠️ No hay registros en {tabla_sql}")
                    return 0
                if tabla_sql == 'TRASLADOS':
                    importador = ImportadorTraslado()
                    for i, fila in enumerate(registros, 1):
                        importador.procesar_fila(fila)
                        if i % 100 == 0:
                            print(f"📤 Procesados {i} registros...")
                    tablas = ['OWTR', 'WTR1', 'OITL', 'ITL1', 'OBTN', 'OBTW', 'OITM']
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            print("❌ Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        for t in tablas:
                            try:
                                cursor.execute(f"TRUNCATE TABLE dbo.{t}")
                                print(f"🧹 Tabla dbo.{t} truncada exitosamente.")
                            except Exception as e:
                                print(f"⚠️ Error al truncar dbo.{t}: {e}")
                            bloques = importador.obtener_bloques(t)
                            for j, bloque in enumerate(bloques, 1):
                                if not bloque.strip():
                                    continue
                                try:
                                    print(f"🛰️ Ejecutando bloque {j} de {t}...")
                                    cursor.execute(bloque)
                                except Exception as e:
                                    print(f"❌ Error en bloque {j} ({t}): {e}")
                                    print(f"🔎 Bloque problemático:\n{bloque}")
                        sql.conexion.commit()
                        print(f"✅ Insertados en SQL Server: {total} registros de TRASLADOS")
                        return total
                else:
                    self.importador = Importador()
                    for i, fila in enumerate(registros, 1):
                        self.importador.query_transaccion(fila, tabla_sql)
                        if i % 100 == 0:
                            print(f"📤 Procesados {i} registros...")
                    with ConexionSQL() as sql:
                        if not sql.db_estado:
                            print("❌ Conexión a SQL Server fallida")
                            return 0
                        cursor = sql.cursor
                        try:
                            cursor.execute(f"TRUNCATE TABLE dbo.{tabla_sql}")
                            print(f"🧹 Tabla dbo.{tabla_sql} truncada exitosamente.")
                        except Exception as e:
                            print(f"⚠️ Error al truncar dbo.{tabla_sql}: {e}")
                        bloques = self.importador.query_sql
                        for j, bloque in enumerate(bloques, 1):
                            if not bloque.strip():
                                continue
                            try:
                                print(f"🛰️ Ejecutando bloque {j} de {tabla_sql}...")
                                cursor.execute(bloque)
                            except Exception as e:
                                print(f"❌ Error en bloque {j} ({tabla_sql}): {e}")
                                print(f"🔎 Bloque problemático:\n{bloque}")
                        sql.conexion.commit()
                        print(f"✅ Insertados en SQL Server: {total} registros de {tabla_sql}")
                        return total
        except Exception as e:
            print(f"❌ Error migrando {tabla_sql}: {e}")
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
