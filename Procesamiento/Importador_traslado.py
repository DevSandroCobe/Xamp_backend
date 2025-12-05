class ImportadorTraslado:
    def __init__(self):
        # Almacén de sentencias SQL
        self.inserts = {
            'OWTR': [], 'WTR1': [], 'OITL': [], 'ITL1': [],
            'OBTN': [], 'OBTW': [], 'OITM': []
        }
        
        # --- CORRECCIÓN 1: Agregar las tablas que faltaban en el control de duplicados ---
        self.procesados = {
            'OWTR': set(), # Faltaba esto
            'WTR1': set(), # Faltaba esto
            'OITL': set(),
            'ITL1': set(),
            'OBTN': set(),
            'OBTW': set(),
            'OITM': set()
        }

    def _str(self, val):
        if val is None:
            return "NULL"
        # Limpieza estándar para SQL Server
        clean = str(val).replace('"', '').replace("'", "''")
        return f"'{clean}'"

    # --- TABLAS PADRE (Evitar duplicados) ---

    def agregar_owtr(self, fila):
        # Indices 0-10 (11 columnas)
        doc_entry = fila[0]
        
        # --- CORRECCIÓN 2: Si ya procesamos este documento, salimos ---
        if doc_entry in self.procesados['OWTR']: 
            return

        stmt = f"INSERT INTO OWTR VALUES({','.join([self._str(x) for x in fila[0:11]])})"
        self.inserts['OWTR'].append(stmt)
        self.procesados['OWTR'].add(doc_entry)

    def agregar_wtr1(self, fila):
        # Indices 11-16 (6 columnas)
        # PK: DocEntry (11) + LineNum (12)
        llave = (fila[11], fila[12])
        
        # --- CORRECCIÓN 3: Si ya procesamos esta línea, salimos ---
        if llave in self.procesados['WTR1']: 
            return

        stmt = f"INSERT INTO WTR1 VALUES({','.join([self._str(x) for x in fila[11:17]])})"
        self.inserts['WTR1'].append(stmt)
        self.procesados['WTR1'].add(llave)

    # --- TABLAS RELACIONADAS ---

    def agregar_oitl(self, fila):
        # Indices 17-23 (7 columnas)
        log_entry = fila[17]
        
        # Validar que exista y no esté repetido
        if not log_entry or log_entry in self.procesados['OITL']: 
            return

        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[17:24]])})"
        self.inserts['OITL'].append(stmt)
        self.procesados['OITL'].add(log_entry)

    def agregar_itl1(self, fila):
        # Indices 24-28 (5 columnas)
        # PK compuesta: LogEntry + ItemCode + SysNumber
        # (Asegúrate de que los índices coincidan con tu query SQL)
        llave = (fila[24], fila[25], fila[27])
        
        if not fila[24] or llave in self.procesados['ITL1']: 
            return

        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[24:29]])})"
        self.inserts['ITL1'].append(stmt)
        self.procesados['ITL1'].add(llave)

    def agregar_obtn(self, fila):
        # Indices 29-34 (6 columnas)
        item_code = fila[29]
        dist_number = fila[30]
        llave = (item_code, dist_number)
        
        if not item_code or llave in self.procesados['OBTN']: 
            return
            
        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[29:35]])})"
        self.inserts['OBTN'].append(stmt)
        self.procesados['OBTN'].add(llave)

    def agregar_obtw(self, fila):
        # Indices 35-39 (5 columnas)
        abs_entry = fila[39]
        
        if not abs_entry or abs_entry in self.procesados['OBTW']: 
            return
            
        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[35:40]])})"
        self.inserts['OBTW'].append(stmt)
        self.procesados['OBTW'].add(abs_entry)

    def agregar_oitm(self, fila):
        # Indices 40-47 (Puede variar según tu query, ajusta si es necesario)
        item_code = fila[40]
        
        if not item_code or item_code in self.procesados['OITM']: 
            return
            
        # Asumiendo que tomas desde el 40 hasta el final o rango fijo
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