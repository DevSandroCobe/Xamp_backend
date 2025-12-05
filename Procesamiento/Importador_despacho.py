class ImportadorDespacho:
    def __init__(self):
        self.inserts = {
            'OINV': [],
            'INV1': [],
            'IBT1': [],
            'OBTN': [],
            'OBTW': [],
            'OITL': [],
            'ITL1': [],
            'OITM': []
        }
        # Control de duplicados para evitar errores de Primary Key
        self.procesados = {
            'OINV': set(),
            'INV1': set(),
            'OBTN': set(),
            'OBTW': set(),
            'OITL': set(),
            'ITL1': set(),
            'OITM': set()
        }

    def _str(self, val):
        if val is None:
            return "NULL"
        # Limpieza estándar para SQL
        clean = str(val).replace('"', '').replace("'", "''")
        return f"'{clean}'"

    def agregar_oinv(self, fila):
        # OINV: 14 campos (0-13) - PK: DocEntry
        doc_entry = fila[0]
        if doc_entry in self.procesados['OINV']:
            return

        stmt = f"INSERT INTO OINV VALUES({','.join([self._str(x) for x in fila[0:14]])})"
        self.inserts['OINV'].append(stmt)
        self.procesados['OINV'].add(doc_entry)

    def agregar_inv1(self, fila):
        # INV1: 9 campos (14-22) - PK: DocEntry + LineNum
        llave = (fila[14], fila[18])
        if llave in self.procesados['INV1']:
            return

        stmt = f"INSERT INTO INV1 VALUES({','.join([self._str(x) for x in fila[14:23]])})"
        self.inserts['INV1'].append(stmt)
        self.procesados['INV1'].add(llave)

    def agregar_ibt1(self, fila):
        # IBT1: 7 campos (23-29)
        # Esta tabla vincula lotes con lineas, se insertan todas las filas que vienen.
        stmt = f"INSERT INTO IBT1 VALUES({','.join([self._str(x) for x in fila[23:30]])})"
        self.inserts['IBT1'].append(stmt)

    def agregar_obtn(self, fila):
        # OBTN: 6 campos (30-35) - PK: ItemCode + DistNumber
        item_code = fila[30]
        dist_number = fila[31]
        llave = (item_code, dist_number)
        
        if not item_code or llave in self.procesados['OBTN']:
            return

        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[30:36]])})"
        self.inserts['OBTN'].append(stmt)
        self.procesados['OBTN'].add(llave)

    def agregar_obtw(self, fila):
        # OBTW: 5 campos (36-40) - PK: AbsEntry
        abs_entry = fila[40]
        if not abs_entry or abs_entry in self.procesados['OBTW']:
            return

        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[36:41]])})"
        self.inserts['OBTW'].append(stmt)
        self.procesados['OBTW'].add(abs_entry)

    def agregar_oitl(self, fila):
        # OITL: 7 campos (41-47) - PK: LogEntry
        log_entry = fila[41]
        if not log_entry or log_entry in self.procesados['OITL']:
            return

        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[41:48]])})"
        self.inserts['OITL'].append(stmt)
        self.procesados['OITL'].add(log_entry)

    def agregar_itl1(self, fila):
        # ITL1: 5 campos (48-52) - PK Compuesta
        # LogEntry(48) + ItemCode(49) + SysNumber(51)
        llave = (fila[48], fila[49], fila[51])
        if llave in self.procesados['ITL1']:
            return

        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[48:53]])})"
        self.inserts['ITL1'].append(stmt)
        self.procesados['ITL1'].add(llave)

    def agregar_oitm(self, fila):
        # OITM: 7 campos (53-59) - PK: ItemCode
        item_code = fila[53]
        if not item_code or item_code in self.procesados['OITM']:
            return

        stmt = f"INSERT INTO OITM VALUES({','.join([self._str(x) for x in fila[53:60]])})"
        self.inserts['OITM'].append(stmt)
        self.procesados['OITM'].add(item_code)

    def procesar_fila(self, fila):
        self.agregar_oinv(fila)
        self.agregar_inv1(fila)
        self.agregar_ibt1(fila)
        self.agregar_obtn(fila)
        self.agregar_obtw(fila)
        self.agregar_oitl(fila)
        self.agregar_itl1(fila)
        self.agregar_oitm(fila)

    def obtener_bloques(self, tabla):
        return self.inserts.get(tabla, [])