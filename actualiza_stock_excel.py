import pandas as pd
import mysql.connector

# ==== CONFIGURACIÓN DE CONEXIÓN ====
DB_CONFIG = {
    "host": "10.0.0.10",
    "user": "simecsoft",
    "password": "ela2006",
    "database": "sisinvrequilab2025",
    "charset": "latin1",
    "use_unicode": True,
}

# ==== ARCHIVO DE EXCEL ====
EXCEL_PATH = r"D:\productos.xlsx"

# ==== LECTURA DEL EXCEL ====
df = pd.read_excel(EXCEL_PATH)
df.columns = ["CODIGO", "DESCRIPCION", "CODIGO_ALTERNO", "ORIGEN", "PRESENTACION", "UNIDAD", "PRECIO_VENTA"]

# ==== LIMPIEZA DE TEXTO ====
def limpiar_texto(valor):
    if pd.isna(valor):
        return None
    return (
        str(valor)
        .replace("_x000D_", "")
        .replace("\r", "")
        .replace("\n", "")
        .strip()
    )

df = df.applymap(limpiar_texto)

# ==== CONEXIÓN A MYSQL ====
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

# ==== ACTUALIZAR SOLO PRESENTACION (DESCRIP1) ====
contador = 0

sql = """
    UPDATE stock
    SET DESCRIP1 = %s
    WHERE CODIGO = %s
"""

for _, row in df.iterrows():

    valores = (
        row["PRESENTACION"],
        row["CODIGO"]
    )

    cursor.execute(sql, valores)

    # cuenta solo si realmente se actualizó
    contador += cursor.rowcount

# ==== GUARDAR CAMBIOS ====
conn.commit()

print(f"✅ Proceso finalizado. Registros actualizados: {contador}")

# ==== CIERRE DE CONEXIÓN ====
cursor.close()
conn.close()