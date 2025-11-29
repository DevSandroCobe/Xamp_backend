from datetime import datetime

class ImportadorTraslado:
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

    def _str(self, val):
        if val is None:
            return "NULL"
        # Convertir a string, eliminar comillas dobles y escapar comillas simples
        clean = str(val).replace('"', '').replace("'", "''")
        return f"'{clean}'"

    def agregar_owtr(self, fila):
        stmt = f"INSERT INTO OWTR VALUES({','.join([self._str(x) for x in fila[0:11]])})"
        self.inserts['OWTR'].append(stmt)

    def agregar_wtr1(self, fila):
        stmt = f"INSERT INTO WTR1 VALUES({','.join([self._str(x) for x in fila[11:17]])})"
        self.inserts['WTR1'].append(stmt)

    def agregar_oitl(self, fila):
        stmt = f"INSERT INTO OITL VALUES({','.join([self._str(x) for x in fila[17:24]])})"
        self.inserts['OITL'].append(stmt)

    def agregar_itl1(self, fila):
        stmt = f"INSERT INTO ITL1 VALUES({','.join([self._str(x) for x in fila[24:29]])})"
        self.inserts['ITL1'].append(stmt)

    def agregar_obtn(self, fila):
        stmt = f"INSERT INTO OBTN VALUES({','.join([self._str(x) for x in fila[29:35]])})"
        self.inserts['OBTN'].append(stmt)

    def agregar_obtw(self, fila):
        stmt = f"INSERT INTO OBTW VALUES({','.join([self._str(x) for x in fila[35:40]])})"
        self.inserts['OBTW'].append(stmt)

    def agregar_oitm(self, fila):
        stmt = f"INSERT INTO OITM VALUES({','.join([self._str(x) for x in fila[40:48]])})"
        self.inserts['OITM'].append(stmt)

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
