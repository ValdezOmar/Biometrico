import random
import pandas as pd
from sqlalchemy import create_engine, types
from datetime import datetime

# Configuración de la conexión a la base de datos
DB_CONFIG = {
    'host': '10.0.0.7',
    'user': 'root',
    'password': 'S1st3m4s.',
    'database': 'SIA'
}

# Ruta del archivo Excel
EXCEL_FILE = 'd:/biometrico.xlsx'

def main():
    try:
        # Leer el archivo Excel
        df = pd.read_excel(EXCEL_FILE)
        
        # Seleccionar y renombrar las columnas necesarias
        df = df.iloc[:, [1, 2, 3]]  # Columnas 2, 3, 4 (índices 1, 2, 3)
        df.columns = ['user_id', 'fecha_str', 'hora_str']
        
        # Limpiar y formatear los datos
        df['user_id'] = df['user_id'].astype(str)
        
        # Convertir a datetime
        df['fecha'] = pd.to_datetime(df['fecha_str'], errors='coerce').dt.date
        
        # Procesar la hora - añadir segundos aleatorios si no los tiene
        def procesar_hora(hora_str):
            try:
                # Primero intentamos parsear con segundos
                hora = pd.to_datetime(hora_str, format='%H:%M:%S', errors='coerce').time()
                if pd.isna(hora):
                    # Si falla, parseamos sin segundos y añadimos aleatorios
                    hora_sin_segundos = pd.to_datetime(hora_str, format='%H:%M', errors='coerce')
                    if not pd.isna(hora_sin_segundos):
                        segundos_aleatorios = random.randint(0, 30)
                        hora = hora_sin_segundos.replace(second=segundos_aleatorios).time()
                return hora
            except:                
                return None
        
        df['hora'] = df['hora_str'].apply(procesar_hora)
        
        # Eliminar filas con datos inválidos
        df = df.dropna(subset=['fecha', 'hora'])
        
        # Crear conexión a la base de datos
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        )
        
        # Preparar DataFrame para insertar
        df_to_insert = pd.DataFrame({
            'user_id': df['user_id'],
            'fecha': df['fecha'],
            'hora': df['hora'],
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        })
        
        # Definir tipos de datos para las columnas
        dtype = {
            'fecha': types.DATE,
            'hora': types.TIME,
            'created_at': types.TIMESTAMP,
            'updated_at': types.TIMESTAMP
        }
        
        # Insertar datos en la base de datos
        if not df_to_insert.empty:
            df_to_insert.to_sql(
                name='rh_asistencias',
                con=engine,
                if_exists='append',
                index=False,
                dtype=dtype
            )
            print(f"Se insertaron {len(df_to_insert)} registros correctamente.")
        else:
            print("No hay datos válidos para insertar.")
            
    except Exception as e:
        print(f"Ocurrió un error: {str(e)}")

if __name__ == '__main__':
    main()