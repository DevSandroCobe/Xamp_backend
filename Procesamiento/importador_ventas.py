class ImportadorVentas:
    def __init__(self):
        self.inserts = {
            'ODLN': [],
            'DLN1': [],
            'IBT1': [],
            'OBTN': [],
            'OBTW': [],
            'OITL': [],
            'ITL1': [],
            'OITM': []
        }

    def _str(self, val):
        return f"'{str(val)}'" if val is not None else "NULL"
    

    def agregar_odln(self, fila):
        # ODLN: 13 campos (0-12)
        stmt = f"INSERT INTO ODLN VALUES({','.join([self._str(x) for x in fila[0:13]])})"
        self.inserts['ODLN'].append(stmt)

    def agregar_dln1(self, fila):
        # DLN1: 7 campos (13-19)
        stmt = f"INSERT INTO DLN1 VALUES({','.join([self._str(x) for x in fila[13:20]])})"
        self.inserts['DLN1'].append(stmt)

    def agregar_ibt1(self, fila):
        # IBT1: 7 campos (20-26)
        stmt = f"INSERT INTO IBT1 VALUES({','.join([self._str(x) for x in fila[20:27]])})"
        self.inserts['IBT1'].append(stmt)

    def agregar_obtn(self, fila):
        # OBTN: 6 campos (27-32)
        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[27:33]])})"
        self.inserts['OBTN'].append(stmt)

    def agregar_obtw(self, fila):
        # OBTW: 5 campos (33-37)
        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[33:38]])})"
        self.inserts['OBTW'].append(stmt)

    def agregar_oitl(self, fila):
        # OITL: 7 campos (38-44)
        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[38:45]])})"
        self.inserts['OITL'].append(stmt)

    def agregar_itl1(self, fila):
        # ITL1: 5 campos (45-49)
        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[45:50]])})"
        self.inserts['ITL1'].append(stmt)

    def agregar_oitm(self, fila):
        # OITM: 7 campos (50-56)
        stmt = f"INSERT INTO OITM VALUES({','.join([self._str(x) for x in fila[50:57]])})"
        self.inserts['OITM'].append(stmt)

    def procesar_fila(self, fila):
        print(f"[DEBUG] Fila recibida: {fila}")
        # Cabecera
        self.agregar_odln(fila)
        # Líneas
        self.agregar_dln1(fila)
        # Movimientos de inventario
        self.agregar_ibt1(fila)
        self.agregar_obtn(fila)
        self.agregar_obtw(fila)
        self.agregar_oitl(fila)
        self.agregar_itl1(fila)
        # Maestro
        self.agregar_oitm(fila)
        
    def obtener_bloques(self, tabla):
        return self.inserts.get(tabla, [])
