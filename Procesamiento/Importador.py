from datetime import datetime

class Importador:
    def __init__(self):
        self.query_sql = ["USE SAP "]
        self.estado = "procesando"
        self.contador = 0
        self.i = 0
        self.limite = 400

    def _agregar_insert(self, tabla, valores):
        if self.contador >= self.limite:
            print(f"🧱 Límite alcanzado ({self.limite}) en bloque {self.i} de {tabla}. Generando nuevo bloque...")
            self.contador = 0
            self.i += 1
            self.query_sql.append("")

        insert_stmt = f"\nINSERT INTO {tabla} VALUES(" + ",".join(valores) + ")"
        self.query_sql[self.i] += insert_stmt
        self.contador += 1

    def _str(self, val):
        if val is None or val == '' or str(val).lower() == 'none':
            return 'NULL'
        return f"'{str(val)}'"

    def query_transaccion(self, reg_hana, tabla):
        print(f"📥 Recibido registro para tabla {tabla}: {reg_hana}")
        try:
            if tabla == "OITM":
                # OITM: ItemCode, ItemName, FrgnName, U_SYP_CONCENTRACION, U_SYP_FORPR, U_SYP_FFDET, U_SYP_FABRICANTE
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # ItemCode
                    self._str(reg_hana[1]),  # ItemName
                    self._str(reg_hana[2]),  # FrgnName
                    self._str(reg_hana[3]),  # U_SYP_CONCENTRACION
                    self._str(reg_hana[4]),  # U_SYP_FORPR
                    self._str(reg_hana[5]),  # U_SYP_FFDET
                    self._str(reg_hana[6])   # U_SYP_FABRICANTE
                ])
            elif tabla == "OWHS":
                # OWHS: WhsCode, WhsName, TaxOffice
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # WhsCode
                    self._str(reg_hana[1]),  # WhsName
                    self._str(reg_hana[2])   # TaxOffice
                ])
            elif tabla == "OWTR":
                # OWTR: DocEntry, DocNum, DocDate, Filler, ToWhsCode, U_SYP_MDTD, U_SYP_MDSD, U_SYP_MDCD, ObjType, CardName, U_BPP_FECINITRA
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # DocEntry
                    self._str(reg_hana[1]),  # DocNum
                    self._str(reg_hana[2]),  # DocDate
                    self._str(reg_hana[3]),  # Filler
                    self._str(reg_hana[4]),  # ToWhsCode
                    self._str(reg_hana[5]),  # U_SYP_MDTD
                    self._str(reg_hana[6]),  # U_SYP_MDSD
                    self._str(reg_hana[7]),  # U_SYP_MDCD
                    self._str(reg_hana[8]),  # ObjType
                    self._str(reg_hana[9]),  # CardName
                    self._str(reg_hana[10])  # U_BPP_FECINITRA
                ])
            elif tabla == "WTR1":
                # WTR1: DocEntry, LineNum, ItemCode, Dscription, WhsCode, ObjType
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # DocEntry
                    self._str(reg_hana[1]),  # LineNum
                    self._str(reg_hana[2]),  # ItemCode
                    self._str(reg_hana[3]),  # Dscription
                    self._str(reg_hana[4]),  # WhsCode
                    self._str(reg_hana[5])   # ObjType
                ])
            elif tabla == "OITL":
                # OITL: LogEntry, ItemCode, DocEntry, DocLine, DocType, StockEff, LocCode
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # LogEntry
                    self._str(reg_hana[1]),  # ItemCode
                    self._str(reg_hana[2]),  # DocEntry
                    self._str(reg_hana[3]),  # DocLine
                    self._str(reg_hana[4]),  # DocType
                    self._str(reg_hana[5]),  # StockEff
                    self._str(reg_hana[6])   # LocCode
                ])
            elif tabla == "ODLN":
                # ODLN: DocEntry, ObjType, DocNum, CardCode, CardName, NumAtCard, DocDate, TaxDate, U_SYP_MDTD, U_SYP_MDSD, U_SYP_MDCD, U_COB_LUGAREN, U_BPP_FECINITRA
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # DocEntry
                    self._str(reg_hana[1]),  # ObjType
                    self._str(reg_hana[2]),  # DocNum
                    self._str(reg_hana[3]),  # CardCode
                    self._str(reg_hana[4]),  # CardName
                    self._str(reg_hana[5]),  # NumAtCard
                    self._str(reg_hana[6]),  # DocDate
                    self._str(reg_hana[7]),  # TaxDate
                    self._str(reg_hana[8]),  # U_SYP_MDTD
                    self._str(reg_hana[9]),  # U_SYP_MDSD
                    self._str(reg_hana[10]), # U_SYP_MDCD
                    self._str(reg_hana[11]), # U_COB_LUGAREN
                    self._str(reg_hana[12])  # U_BPP_FECINITRA
                ])
            elif tabla == "OINV":
                # OINV: DocEntry, NumAtCard, U_SYP_NGUIA, ObjType, DocNum, CardCode, CardName, DocDate, TaxDate, U_SYP_MDTD, U_SYP_MDSD, U_SYP_MDCD, U_COB_LUGAREN, U_BPP_FECINITRA
                print(f"[DEBUG OINV] Datos recibidos: {reg_hana}")
                valores = [
                    self._str(reg_hana[0]),  # DocEntry
                    self._str(reg_hana[1]),  # NumAtCard
                    self._str(reg_hana[2]),  # U_SYP_NGUIA
                    self._str(reg_hana[3]),  # ObjType
                    self._str(reg_hana[4]),  # DocNum
                    self._str(reg_hana[5]),  # CardCode
                    self._str(reg_hana[6]),  # CardName
                    self._str(reg_hana[7]),  # DocDate
                    self._str(reg_hana[8]),  # TaxDate
                    self._str(reg_hana[9]),  # U_SYP_MDTD
                    self._str(reg_hana[10]), # U_SYP_MDSD
                    self._str(reg_hana[11]), # U_SYP_MDCD
                    self._str(reg_hana[12]), # U_COB_LUGAREN
                    self._str(reg_hana[13])  # U_BPP_FECINITRA
                ]
                insert_stmt = f"\nINSERT INTO {tabla} VALUES(" + ",".join(valores) + ")"
                print(f"[DEBUG OINV] SQL generado: {insert_stmt}")
                self._agregar_insert(tabla, valores)
            elif tabla == "OBTW":
                # OBTW: ItemCode, MdAbsEntry, WhsCode, Location, AbsEntry
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # ItemCode
                    self._str(reg_hana[1]),  # MdAbsEntry
                    self._str(reg_hana[2]),  # WhsCode
                    self._str(reg_hana[3]),  # Location
                    self._str(reg_hana[4])   # AbsEntry
                ])
            elif tabla == "OBTN":
                # OBTN: ItemCode, DistNumber, SysNumber, AbsEntry, MnfSerial, ExpDate
                try:
                    fecha = datetime.strptime(reg_hana[5], "%Y-%m-%d %H:%M:%S")
                    fecha_formateada = fecha.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    fecha_formateada = ""
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # ItemCode
                    self._str(reg_hana[1]),  # DistNumber
                    self._str(reg_hana[2]),  # SysNumber
                    self._str(reg_hana[3]),  # AbsEntry
                    self._str(reg_hana[4]),  # MnfSerial
                    self._str(fecha_formateada) # ExpDate
                ])
            elif tabla == "ITL1":
                # ITL1: LogEntry, ItemCode, Quantity, SysNumber, MdAbsEntry
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # LogEntry
                    self._str(reg_hana[1]),  # ItemCode
                    self._str(reg_hana[2]),  # Quantity
                    self._str(reg_hana[3]),  # SysNumber
                    self._str(reg_hana[4])   # MdAbsEntry
                ])
            elif tabla == "IBT1":
                # IBT1: ItemCode, BatchNum, WhsCode, BaseEntry, BaseType, BaseLinNum, Quantity
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # ItemCode
                    self._str(reg_hana[1]),  # BatchNum
                    self._str(reg_hana[2]),  # WhsCode
                    self._str(reg_hana[3]),  # BaseEntry
                    self._str(reg_hana[4]),  # BaseType
                    self._str(reg_hana[5]),  # BaseLinNum
                    self._str(reg_hana[6])   # Quantity
                ])
            elif tabla == "DLN1":
                # DLN1: DocEntry, ObjType, WhsCode, ItemCode, LineNum, Dscription, UomCode
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # DocEntry
                    self._str(reg_hana[1]),  # ObjType
                    self._str(reg_hana[2]),  # WhsCode
                    self._str(reg_hana[3]),  # ItemCode
                    self._str(reg_hana[4]),  # LineNum
                    self._str(reg_hana[5]),  # Dscription
                    self._str(reg_hana[6])   # UomCode
                ])
            elif tabla == "INV1":
                # INV1: DocEntry, ObjType, WhsCode, ItemCode, LineNum, Dscription, UomCode, BaseType, BaseEntry
                self._agregar_insert(tabla, [
                    self._str(reg_hana[0]),  # DocEntry
                    self._str(reg_hana[1]),  # ObjType
                    self._str(reg_hana[2]),  # WhsCode
                    self._str(reg_hana[3]),  # ItemCode
                    self._str(reg_hana[4]),  # LineNum
                    self._str(reg_hana[5]),  # Dscription
                    self._str(reg_hana[6]),  # UomCode
                    self._str(reg_hana[7]),  # BaseType
                    self._str(reg_hana[8])   # BaseEntry
                ])

        except IndexError as e:
            print(f"❌ ERROR en {tabla}: índice fuera de rango en reg_hana → {e}")
        except Exception as e:
            print(f"❌ ERROR inesperado en {tabla}: {e}")
