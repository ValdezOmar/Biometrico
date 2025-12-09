import pandas as pd
import mysql.connector

# ==== CONFIGURACIÓN DE CONEXIÓN ====
DB_CONFIG = {
    "host": "10.0.0.10",
    "user": "simecsoft",
    "password": "ela2006",
    "database": "sisinvnovanexa2025",
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

# ==== INSERTAR O ACTUALIZAR SI YA EXISTE ====
contador = 0
for _, row in df.iterrows():
    sql = """
        INSERT INTO stock (CODIGO, DESCRIP, DESCRIP1, UNIDAD, CODIGO1, ORIGEN)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            DESCRIP = VALUES(DESCRIP),
            DESCRIP1 = VALUES(DESCRIP1),
            UNIDAD = VALUES(UNIDAD),
            CODIGO1 = VALUES(CODIGO1),
            ORIGEN = VALUES(ORIGEN)
    """

    valores = (
        row["CODIGO"],
        row["DESCRIPCION"],
        row["PRESENTACION"],
        row["UNIDAD"],
        row["CODIGO_ALTERNO"],
        row["ORIGEN"],
    )

    cursor.execute(sql, valores)
    contador += 1

conn.commit()
print(f"✅ Proceso finalizado. Registros insertados o actualizados: {contador}")

# ==== CIERRE DE CONEXIÓN ====
cursor.close()
conn.close()
