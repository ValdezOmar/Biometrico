import pymysql
import pandas as pd
import math

DB_CONFIG = {
    "host": "10.0.0.10",
    "user": "simecsoft",
    "password": "ela2006",
    "database": "sisinvnovanexa2025",
    "charset": "latin1",
    "use_unicode": True,
}

TIPRE = 1
excel_path = r"D:\precios.xlsx"

# -------------------------------
# LEER EXCEL
# -------------------------------
df = pd.read_excel(excel_path)
df.columns = [c.strip().upper() for c in df.columns]

# -------------------------------
# CONEXIÓN MYSQL
# -------------------------------
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()

print("Conectado a MySQL...")

# Contador de actualizaciones
modificados = 0

# -------------------------------
# RECORRER EXCEL
# -------------------------------
for index, row in df.iterrows():

    codigo = str(row["CODIGO"]).strip()
    precio = row["PRECIO"]

    # Evitar códigos vacíos
    if not codigo:
        continue

    # Evitar precios NaN, None, vacíos, cero
    if precio is None or (isinstance(precio, float) and math.isnan(precio)):
        continue

    if float(precio) <= 0:
        continue

    # Verificar si el código existe
    cursor.execute("""
        SELECT CODIGO 
        FROM preciosdet 
        WHERE TIPRE = %s AND CODIGO = %s
    """, (TIPRE, codigo))

    existe = cursor.fetchone()

    if not existe:
        # No actualizar si no existe
        continue

    # HACER UPDATE
    cursor.execute("""
        UPDATE preciosdet
        SET PRECIO = %s
        WHERE TIPRE = %s AND CODIGO = %s
    """, (float(precio), TIPRE, codigo))

    modificados += 1
    print(f"Modificado: {codigo} -> {precio}")

# Guardar cambios
conn.commit()
cursor.close()
conn.close()

print("================================")
print(f"TOTAL REGISTROS MODIFICADOS: {modificados}")
print("================================")
