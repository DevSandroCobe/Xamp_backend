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

    def _str(self, val):
        if val is None:
            return "NULL"
        # Convertir a string, eliminar comillas dobles y escapar comillas simples
        clean = str(val).replace('"', '').replace("'", "''")
        return f"'{clean}'"

    def agregar_oinv(self, fila):
        # OINV: 14 campos (0-13)
        stmt = f"INSERT INTO OINV VALUES({','.join([self._str(x) for x in fila[0:14]])})"
        self.inserts['OINV'].append(stmt)

    def agregar_inv1(self, fila):
        # INV1: 9 campos (14-22)
        stmt = f"INSERT INTO INV1 VALUES({','.join([self._str(x) for x in fila[14:23]])})"
        self.inserts['INV1'].append(stmt)

    def agregar_ibt1(self, fila):
        # IBT1: 7 campos (23-29)
        stmt = f"INSERT INTO IBT1 VALUES({','.join([self._str(x) for x in fila[23:30]])})"
        self.inserts['IBT1'].append(stmt)

    def agregar_obtn(self, fila):
        # OBTN: 6 campos (30-35)
        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[30:36]])})"
        self.inserts['OBTN'].append(stmt)

    def agregar_obtw(self, fila):
        # OBTW: 5 campos (36-40)
        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[36:41]])})"
        self.inserts['OBTW'].append(stmt)

    def agregar_oitl(self, fila):
        # OITL: 7 campos (41-47)
        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[41:48]])})"
        self.inserts['OITL'].append(stmt)

    def agregar_itl1(self, fila):
        # ITL1: 5 campos (48-52)
        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[48:53]])})"
        self.inserts['ITL1'].append(stmt)

    def agregar_oitm(self, fila):
        # OITM: 7 campos (53-59)
        stmt = f"INSERT INTO OITM VALUES({','.join([self._str(x) for x in fila[53:60]])})"
        self.inserts['OITM'].append(stmt)

    def procesar_fila(self, fila):
        print(f"[DEBUG] Fila recibida: {fila}")
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
