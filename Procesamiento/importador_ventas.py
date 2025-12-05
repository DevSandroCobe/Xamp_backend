class ImportadorVentas:
    def __init__(self):
        # Aquí guardaremos las sentencias SQL generadas
        self.inserts = {
            'ODLN': [], 'DLN1': [], 'IBT1': [], 'OBTN': [],
            'OBTW': [], 'OITL': [], 'ITL1': [], 'OITM': []
        }
        
        # --- CEREBRO DE LA OPERACIÓN: MEMORIA DE PROCESADOS ---
        # Esto sirve para recordar qué ya escribimos y no repetirlo en tablas únicas.
        self.procesados = {
            'ODLN': set(),  # Para Cabeceras (Debe ser único por DocEntry)
            'DLN1': set(),  # Para Líneas (Debe ser único por DocEntry + LineNum)
            'OITM': set(),  # Para Artículos (Debe ser único por ItemCode)
            'OBTN': set(),  # Para Lotes Maestros (PK: ItemCode + DistNumber)
            'OBTW': set(),  # Para Lotes por Almacén (PK: AbsEntry)
            'OITL': set(),  # Para Logs (PK: LogEntry)
            'ITL1': set()   # Para Detalles Logs (PK: LogEntry + ItemCode + SysNumber)
        }

    def _str(self, val):
        """
        CORRECCIÓN DE SEGURIDAD: 
        1. Maneja valores vacíos (None) como NULL.
        2. Escapa las comillas simples (') convirtiéndolas en dobles ('') 
           para que productos como "TUBO 1/2'" no rompan el SQL.
        """
        if val is None:
            return "NULL"
        val_limpio = str(val).replace("'", "''")
        return f"'{val_limpio}'"
    
    # --- TABLAS PADRE (Solo deben insertarse UNA vez) ---

    def agregar_odln(self, fila):
        # ID Único: DocEntry (Columna 0)
        doc_entry = fila[0]
        
        # SI YA LO PROCESÉ, LO IGNORO (Para no duplicar en ODLN)
        if doc_entry in self.procesados['ODLN']:
            return 

        stmt = f"INSERT INTO ODLN VALUES({','.join([self._str(x) for x in fila[0:13]])})"
        self.inserts['ODLN'].append(stmt)
        # Lo marco como procesado
        self.procesados['ODLN'].add(doc_entry)

    def agregar_dln1(self, fila):
        # ID Único: DocEntry + LineNum (Columnas 13 y 17)
        llave_linea = (fila[13], fila[17])
        
        # SI YA PROCESÉ ESTA LÍNEA ESPECÍFICA, LA IGNORO
        if llave_linea in self.procesados['DLN1']:
            return

        stmt = f"INSERT INTO DLN1 VALUES({','.join([self._str(x) for x in fila[13:20]])})"
        self.inserts['DLN1'].append(stmt)
        self.procesados['DLN1'].add(llave_linea)

    def agregar_oitm(self, fila):
        # ID Único: ItemCode (Columna 50)
        item_code = fila[50]
        
        if item_code in self.procesados['OITM']:
            return

        stmt = f"INSERT INTO OITM VALUES({','.join([self._str(x) for x in fila[50:57]])})"
        self.inserts['OITM'].append(stmt)
        self.procesados['OITM'].add(item_code)

    # --- TABLAS HIJO (Detalles / Lotes) ---
    # Estas SÍ deben repetirse tantas veces como vengan en la query de HANA.
    
    def agregar_ibt1(self, fila):
        # IBT1 es el vínculo entre la línea y el lote.
        # Si una línea tiene 3 lotes, HANA trae 3 filas. 
        # AQUÍ NECESITAMOS INSERTAR LAS 3 VECES. NO HAY IF.
        stmt = f"INSERT INTO IBT1 VALUES({','.join([self._str(x) for x in fila[20:27]])})"
        self.inserts['IBT1'].append(stmt)

    # --- TABLAS MAESTRAS DE LOTES (Con protección básica) ---

    def agregar_obtn(self, fila):
        # OBTN: ItemCode (27), DistNumber (28), SysNumber (29), AbsEntry (30), MnfSerial (31), ExpDate (32)
        # PK de OBTN es: ItemCode + DistNumber (combinación única)
        item_code = fila[27]
        dist_number = fila[28]
        llave_obtn = (item_code, dist_number)
        
        if llave_obtn in self.procesados['OBTN']: 
            return
        
        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[27:33]])})"
        self.inserts['OBTN'].append(stmt)
        self.procesados['OBTN'].add(llave_obtn)

    def agregar_obtw(self, fila):
        abs_entry = fila[37]
        if abs_entry in self.procesados['OBTW']: return
        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[33:38]])})"
        self.inserts['OBTW'].append(stmt)
        self.procesados['OBTW'].add(abs_entry)

    def agregar_oitl(self, fila):
        log_entry = fila[38]
        if log_entry in self.procesados['OITL']: return
        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[38:45]])})"
        self.inserts['OITL'].append(stmt)
        self.procesados['OITL'].add(log_entry)

    def agregar_itl1(self, fila):
        llave_itl1 = (fila[45], fila[46], fila[48])
        if llave_itl1 in self.procesados['ITL1']: return
        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[45:50]])})"
        self.inserts['ITL1'].append(stmt)
        self.procesados['ITL1'].add(llave_itl1)

    def procesar_fila(self, fila):
        # Al recibir UNA fila de HANA (que contiene datos mezclados),
        # la repartimos a las tablas correspondientes aplicando la lógica de arriba.
        
        # 1. Cabecera (Se filtra si es repetida)
        self.agregar_odln(fila)
        
        # 2. Línea de Artículo (Se filtra si es repetida)
        self.agregar_dln1(fila)
        
        # 3. Transacción de Lote (SIEMPRE ENTRA, aquí está la data "relacionada")
        self.agregar_ibt1(fila)
        
        # 4. Resto de tablas maestras (Se filtran si son repetidas)
        self.agregar_obtn(fila)
        self.agregar_obtw(fila)
        self.agregar_oitl(fila)
        self.agregar_itl1(fila)
        self.agregar_oitm(fila)
        
    def obtener_bloques(self, tabla):
        return self.inserts.get(tabla, [])