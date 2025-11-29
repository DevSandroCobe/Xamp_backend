from datetime import datetime, timedelta
from Conexion.conexion_hana import ConexionHANA
from Conexion.conexion_sql import ConexionSQL
from Procesamiento.Importador import Importador
from Config.conexion_config import CONFIG_HANA


class Migrador:
    def __init__(self, fecha_str):
        self.fecha = datetime.strptime(fecha_str, "%Y-%m-%d")
        self.fecha_inicio = self.fecha.replace(hour=0, minute=0, second=0)
        self.fecha_fin = self.fecha_inicio + timedelta(days=1) - timedelta(seconds=1)
        self.importador = Importador()
        self.tablas_objetivo = [
            'OITM', 'OBTW', 'OBTN',  'OWHS', 'OINV',
            'INV1', 'OITL', 'ITL1', 'ODLN', 'DLN1', 'OWTR', 'WTR1','IBT1'
        ]
        self.queries = self._construir_queries()

    def _esquema(self, tabla):
        return CONFIG_HANA["schema"]

    def _formato_fecha_hana(self, columna):
        return f'''
            YEAR({columna}) || '-' ||
            (CASE WHEN MONTH({columna}) < 10 THEN '0' ELSE '' END) || MONTH({columna}) || '-' ||
            (CASE WHEN DAYOFMONTH({columna}) < 10 THEN '0' ELSE '' END) || DAYOFMONTH({columna})
        '''


    def _construir_queries(self):
        esq = CONFIG_HANA["schema"]
        return {
            'OBTW': f'''
                SELECT T0."ItemCode", T0."MdAbsEntry", T0."WhsCode", T0."Location", T0."AbsEntry" 
                FROM {self._esquema("OBTW")}.OBTW T0''',
            
            'OBTN': f'''
                SELECT T0."ItemCode", T0."DistNumber", T0."SysNumber", T0."AbsEntry", T0."MnfSerial",
                       {self._formato_fecha_hana('T0."ExpDate"')} AS "ExpDate"
                FROM {self._esquema("OBTN")}.OBTN T0
                WHERE "ExpDate" > '{self.fecha:%Y-%m-%d}' ''',
           
           'IBT1': f'''
                SELECT T0."ItemCode", T0."BatchNum", T0."WhsCode", T0."BaseEntry", T0."BaseType", T0."BaseLinNum", T0."Quantity"
                FROM {self._esquema("IBT1")}.IBT1 T0
                WHERE "DocDate" = '{self.fecha:%Y-%m-%d}'
            ''',
   
            'OITM': f'''SELECT T0."ItemCode", T0."ItemName", T0."FrgnName", T0."U_SYP_CONCENTRACION", T0."U_SYP_FORPR", T0."U_SYP_FFDET", T0."U_SYP_FABRICANTE" 
                FROM {self._esquema("OITM")}.OITM T0''',
        
            'OWHS': f'''
                SELECT  T0."WhsCode", T0."WhsName", T0."TaxOffice"
                FROM {self._esquema("OWHS")}.OWHS T0''',
        
            'OINV': f'''
                SELECT  T0."DocEntry",T0."ObjType",T0."DocNum",T0."CardCode",T0."CardName",T0."NumAtCard",T0."DocDate",
                    T0."TaxDate",T0."U_SYP_MDTD",T0."U_SYP_MDSD",T0."U_SYP_MDCD",T0."U_COB_LUGAREN",T0."U_BPP_FECINITRA"
                FROM {self._esquema("OINV")}.OINV T0
                WHERE T0."CANCELED" = 'N'
                AND T0."DocDate" BETWEEN '{self.fecha_inicio:%Y-%m-%d}' AND '{self.fecha_fin:%Y-%m-%d}' ''',
            
            'INV1': f'''
                SELECT  T0."DocEntry", T0."ObjType", T0."WhsCode", T0."ItemCode", T0."LineNum", T0."Dscription",
                    T0."UomCode", T0."BaseType", T0."BaseEntry"
                FROM {self._esquema("OINV")}.OINV T1
                INNER JOIN {self._esquema("INV1")}.INV1 T0 ON T0."DocEntry" = T1."DocEntry"
                WHERE T1."CANCELED" = 'N'
                AND T1."DocDate" = '{self.fecha:%Y-%m-%d}'
            ''',

            'OITL': f'''
                SELECT  T0."LogEntry", T0."ItemCode", T0."DocEntry", T0."DocLine", T0."DocType", T0."StockEff", T0."LocCode"
                FROM {self._esquema("OITL")}.OITL T0
                WHERE T0."DocDate" = '{self.fecha:%Y-%m-%d}'
            ''',


           'ITL1': f'''
                SELECT  T0."LogEntry", T0."ItemCode", T0."Quantity", T0."SysNumber", T0."MdAbsEntry"
                FROM {self._esquema("OITL")}.OITL T1
                INNER JOIN {self._esquema("ITL1")}.ITL1 T0 ON T0."LogEntry" = T1."LogEntry"
                WHERE  T1."DocDate" > '{self.fecha:%Y-%m-%d}'   
            ''',

            'ODLN': f'''
                SELECT  T0."DocEntry", T0."ObjType", T0."DocNum", T0."CardCode",
                    CASE
                        WHEN T0."CardCode" = 'C20608815466' THEN 'DROGUERIA JOSUE S S.A.C.'
                        WHEN T0."CardCode" = 'C20612232360' THEN 'INVERSIONES KAELI S SOCIEDAD COMERCIAL DE RESPONSABILIDAD LIMITADA'
                        WHEN T0."CardCode" = 'C20611448971' THEN 'ALISSON'
                        ELSE T0."CardName"
                    END AS "CardName",
                    T0."NumAtCard",
                    {self._formato_fecha_hana('T0."DocDate"')},
                    {self._formato_fecha_hana('T0."TaxDate"')},
                    T0."U_SYP_MDTD", T0."U_SYP_MDSD", T0."U_SYP_MDCD", T0."U_COB_LUGAREN",
                    {self._formato_fecha_hana('T0."U_BPP_FECINITRA"')}
                FROM {self._esquema("ODLN")}.ODLN T0
                WHERE T0."CANCELED" = 'N'
                AND T0."U_COB_LUGAREN" in ('16', '15')
                AND T0."DocDate" = '{self.fecha:%Y-%m-%d}'
                AND T0."CardCode" NOT IN ('C20611448971')
                ORDER BY T0."DocEntry" ASC
            ''',

            'DLN1': f'''
                SELECT T0."DocEntry", T0."ObjType", T0."WhsCode", T0."ItemCode", T0."LineNum",
                    T0."Dscription", T0."UomCode"
                FROM {self._esquema("ODLN")}.ODLN T1
                INNER JOIN {self._esquema("DLN1")}.DLN1 T0 ON T0."DocEntry" = T1."DocEntry"
                WHERE T1."CANCELED" = 'N'
                AND T1."U_SYP_STATUS" = 'V'
                AND T1."U_COB_LUGAREN" IN ('16', '15')
                AND T1."DocDate" = '{self.fecha:%Y-%m-%d}'
            ''',
                
            'OWTR': f'''
                SELECT T0."DocEntry", T0."DocNum", {self._formato_fecha_hana('T0."DocDate"')},
                    T0."Filler", T0."ToWhsCode", T0."U_SYP_MDTD", T0."U_SYP_MDSD", T0."U_SYP_MDCD",
                    T0."ObjType", T0."CardName", {self._formato_fecha_hana('T0."U_BPP_FECINITRA"')}
                FROM {self._esquema("OWTR")}.OWTR T0
                WHERE T0."CANCELED" = 'N'
                AND T0."U_SYP_STATUS" = 'V'
                AND T0."U_SYP_MDSD" IS NOT NULL
                AND T0."U_SYP_MDCD" IS NOT NULL
                AND T0."Filler" IN ('15', '16')
                AND T0."ToWhsCode" IN ('01', '09', 'ALM07')
                AND T0."DocDate" = '{self.fecha:%Y-%m-%d}'
            ''',

           'WTR1': f'''
                SELECT T0."DocEntry", T0."LineNum", T0."ItemCode", T0."Dscription",
                    T0."WhsCode", T0."ObjType"
                FROM {self._esquema("OWTR")}.OWTR T1
                INNER JOIN {self._esquema("WTR1")}.WTR1 T0 ON T0."DocEntry" = T1."DocEntry"
                WHERE T1."CANCELED" = 'N'
                AND T1."U_SYP_STATUS" = 'V'
                AND T1."U_SYP_MDSD" IS NOT NULL
                AND T1."U_SYP_MDCD" IS NOT NULL
                AND T1."Filler" IN ('15', '16')
                AND T1."ToWhsCode" IN ('01', '09', 'ALM07')
                AND T1."DocDate" = '{self.fecha:%Y-%m-%d}'
            '''
        }

    def migracion_hana_sql(self, query: str, tabla_sql: str) -> int:
        print(f"\n🚀 Migrando {tabla_sql}...")

        try:
            with ConexionHANA(query) as hana:
                if not hana.db_estado:
                    raise RuntimeError("Conexión a HANA fallida")

                registros = hana.obtener_tabla()
                total = len(registros)

                if not registros:
                    print(f"⚠️ No hay registros en {tabla_sql}")
                    return 0

                print(f"📥 Registros extraídos de HANA: {total}")

                # Reset del importador por cada tabla
                self.importador = Importador()

                for i, fila in enumerate(registros, 1):
                    self.importador.query_transaccion(fila, tabla_sql)
                    if i % 100 == 0:
                        print(f"📤 Procesados {i} registros...")

            with ConexionSQL() as sql:
                if not sql.db_estado:
                    raise RuntimeError("Conexión a SQL Server fallida")

                cursor = sql.cursor  # ✅ acceso correcto al cursor

                # 🚨 TRUNCATE de la tabla en SQL antes de insertar nuevos datos
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
                        print(f"🔎 Bloque problemático:\n{bloque}")  # 👀 útil para depuración

                sql.conexion.commit()  # ✅ commit correcto
                print(f"✅ Insertados en SQL Server: {total} registros de {tabla_sql}")
                return total

        except Exception as e:
            print(f"❌ Error migrando {tabla_sql}: {e}")
            return 0

    
    def ejecutar_reset(self) -> bool:
        try:
            with ConexionSQL() as sql:
                if sql.valida_conexion():
                    sql.ejecutar(self.queries['reset'])
                    print("🔁 Reset exitoso")
                    return True
        except Exception as e:
            print(f"❌ Error en reset: {e}")
        return False

    def migrar_tabla(self, tabla: str) -> tuple:
        if tabla not in self.queries:
            return 0, f"⚠️ Tabla {tabla} no encontrada"
        cantidad = self.migracion_hana_sql(self.queries[tabla], tabla)
        return cantidad, f"✅ Migración de {tabla}: {cantidad} registros"

    def migrar_todas(self) -> list:
        resultados = []
        if not self.ejecutar_reset():
            return [{"tabla": "reset", "exito": False, "mensaje": "❌ Falló el reset de SQL"}]

        for tabla in self.tablas_objetivo:
                if tabla == "ITL1":
                    # Migrar ITL1 solo para los DocEntry de OWTR
                    owtr_docentries = self._obtener_docentries_owtr()
                    total_itl1 = 0
                    for docentry in owtr_docentries:
                        cantidad, mensaje = self.migrar_tabla_itl1(docentry)
                        resultados.append({
                            "tabla": "ITL1",
                            "docentry": docentry,
                            "fecha": self.fecha.strftime("%Y-%m-%d"),
                            "registros": cantidad,
                            "mensaje": mensaje,
                            "exito": cantidad > 0
                        })
                        total_itl1 += cantidad
                else:
                    cantidad, mensaje = self.migrar_tabla(tabla)
                    resultados.append({
                        "tabla": tabla,
                        "fecha": self.fecha.strftime("%Y-%m-%d"),
                        "registros": cantidad,
                        "mensaje": mensaje,
                        "exito": cantidad > 0
                    })
                return resultados

    def _obtener_docentries_owtr(self):
            # Extrae los DocEntry de OWTR para la fecha indicada
            query = self.queries["OWTR"]
            docentries = []
            try:
                with ConexionHANA(query) as hana:
                    if hana.db_estado:
                        registros = hana.obtener_tabla()
                        for fila in registros:
                            docentries.append(fila[0])  # DocEntry suele estar en la primera columna
            except Exception as e:
                print(f"❌ Error obteniendo DocEntry de OWTR: {e}")
            return docentries

    def migrar_tabla_itl1(self, docentry) -> tuple:
            # Migrar ITL1 solo para el DocEntry relacionado
            query = f'''
                SELECT T0."LogEntry", T0."ItemCode", T0."Quantity", T0."SysNumber", T0."MdAbsEntry"
                FROM {self._esquema("OITL")}.OITL T1
                INNER JOIN {self._esquema("ITL1")}.ITL1 T0 ON T0."LogEntry" = T1."LogEntry"
                WHERE T1."DocEntry" = {docentry}
            '''
            cantidad = self.migracion_hana_sql(query, "ITL1")
            return cantidad, f"✅ Migración de ITL1 para OWTR DocEntry {docentry}: {cantidad} registros"
