class ImportadorOrganoleptico:
    def __init__(self):
        self.inserts = {
            'OWTR': [],
            'WTR1': [],
            'OITL': [],
            'ITL1': [],
            'OBTN': [],
            'OBTW': [],
            'OITM': []
        }
        # Control de duplicados para evitar errores de Primary Key
        self.procesados = {
            'OWTR': set(),
            'WTR1': set(),
            'OITL': set(),
            'ITL1': set(),
            'OBTN': set(),
            'OBTW': set(),
            'OITM': set()
        }

    def _str(self, val):
        if val is None:
            return "NULL"
        # Limpieza estándar para SQL
        clean = str(val).replace('"', '').replace("'", "''")
        return f"'{clean}'"

    def agregar_owtr(self, fila):
        # OWTR: 11 campos (0-10)
        doc_entry = fila[0]
        if doc_entry in self.procesados['OWTR']:
            return
            
        stmt = f"INSERT INTO OWTR VALUES({','.join([self._str(x) for x in fila[0:11]])})"
        self.inserts['OWTR'].append(stmt)
        self.procesados['OWTR'].add(doc_entry)

    def agregar_wtr1(self, fila):
        # WTR1: 6 campos (11-16) - PK: DocEntry + LineNum
        llave = (fila[11], fila[12])
        if llave in self.procesados['WTR1']:
            return

        stmt = f"INSERT INTO WTR1 VALUES({','.join([self._str(x) for x in fila[11:17]])})"
        self.inserts['WTR1'].append(stmt)
        self.procesados['WTR1'].add(llave)

    def agregar_oitl(self, fila):
        # OITL: 7 campos (17-23) - PK: LogEntry
        log_entry = fila[17]
        if not log_entry or log_entry in self.procesados['OITL']:
            return

        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[17:24]])})"
        self.inserts['OITL'].append(stmt)
        self.procesados['OITL'].add(log_entry)

    def agregar_itl1(self, fila):
        # ITL1: 5 campos (24-28) - PK compuesta
        # Usamos LogEntry(24) + ItemCode(25) + SysNumber(27) como unicidad lógica
        llave = (fila[24], fila[25], fila[27])
        if llave in self.procesados['ITL1']:
            return

        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[24:29]])})"
        self.inserts['ITL1'].append(stmt)
        self.procesados['ITL1'].add(llave)

    def agregar_obtn(self, fila):
        # OBTN: 6 campos (29-34)
        item_code = fila[29]
        dist_number = fila[30]
        llave = (item_code, dist_number)
        
        if not item_code or llave in self.procesados['OBTN']:
            return

        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[29:35]])})"
        self.inserts['OBTN'].append(stmt)
        self.procesados['OBTN'].add(llave)

    def agregar_obtw(self, fila):
        # OBTW: 5 campos (35-39) - PK: AbsEntry
        abs_entry = fila[39]
        if not abs_entry or abs_entry in self.procesados['OBTW']:
            return

        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[35:40]])})"
        self.inserts['OBTW'].append(stmt)
        self.procesados['OBTW'].add(abs_entry)

    def agregar_oitm(self, fila):
        # OITM: 8 campos (40-47) - PK: ItemCode
        item_code = fila[40]
        if not item_code or item_code in self.procesados['OITM']:
            return

        stmt = f"INSERT INTO OITM VALUES({','.join([self._str(x) for x in fila[40:48]])})"
        self.inserts['OITM'].append(stmt)
        self.procesados['OITM'].add(item_code)

    def procesar_fila(self, fila):
        self.agregar_owtr(fila)
        self.agregar_wtr1(fila)
        self.agregar_oitl(fila)
        self.agregar_itl1(fila)
        self.agregar_obtn(fila)
        self.agregar_obtw(fila)
        self.agregar_oitm(fila)

    def obtener_bloques(self, tabla):
        return self.inserts.get(tabla, [])