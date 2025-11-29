# backend/utils/conexion_db.py

import pyodbc

def probar_conexion(nombre, connection_string, query):
    print(f"\n🧪 Probando conexión a {nombre}...")
    try:
        conexion = pyodbc.connect(connection_string, timeout=5)
        cursor = conexion.cursor()
        cursor.execute(query)
        resultado = cursor.fetchone()
        print(f"✅ Conexión a {nombre} exitosa ✔️ - Resultado: {resultado}")
        conexion.close()
    except Exception as e:
        print(f"❌ Error al conectar a {nombre}: {e}")
