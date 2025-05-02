from zk import ZK, const
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
import logging
from contextlib import contextmanager
import sys

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('biometric_sync.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Configuración de directorios
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Configuración del dispositivo biométrico
BIOMETRIC_CONFIG = {
    'ip': '192.168.100.253',
    'port': 4370,
    # 'password': 123456,
    'timeout': 10,
    'force_udp': False,
    'ommit_ping': False
}

def validar_fecha(fecha_str: str) -> Optional[datetime]:
    """Valida y convierte una cadena de fecha al formato correcto."""
    try:
        return datetime.strptime(fecha_str, '%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        logger.warning(f"Fecha inválida: {fecha_str} - Error: {e}")
        return None

@contextmanager
def biometric_connection():
    """Maneja la conexión al dispositivo biométrico con contexto."""
    zk = ZK(**BIOMETRIC_CONFIG)
    conn = None
    try:
        logger.info("🔌 Conectando al dispositivo biométrico...")
        conn = zk.connect()
        conn.disable_device()
        yield conn
    except Exception as e:
        logger.error(f"❌ Error en conexión biométrica: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.enable_device()
            conn.disconnect()
            logger.info("✅ Conexión biométrica cerrada correctamente")

def obtener_registros_biometricos(conn) -> List[Tuple]:
    """Obtiene registros del dispositivo biométrico con manejo de errores."""
    logger.info("📥 Obteniendo registros de asistencia...")
    try:
        registros = conn.get_attendance()
        logger.info(f"📊 Se obtuvieron {len(registros)} registros")
        return registros
    except Exception as e:
        logger.error(f"❌ Error al obtener registros: {e}", exc_info=True)
        return []

def procesar_registro(registro) -> Optional[dict]:
    """Procesa un registro individual y valida sus datos."""
    try:
        fecha_hora = registro.timestamp.strftime('%Y-%m-%d %H:%M:%S')
        if not validar_fecha(fecha_hora):
            raise ValueError(f"Fecha inválida: {fecha_hora}")

        return {
            'uid': registro.uid,
            'user_id': registro.user_id,
            'fecha_hora': fecha_hora,
            'estado': registro.punch,
            'tipo': registro.status
        }
    except Exception as e:
        logger.warning(f"⚠️ Registro inválido: {e}")
        return None

def guardar_registros_en_txt(registros: List[dict]) -> Tuple[int, int]:
    """Guarda los registros procesados en un archivo TXT en el disco."""
    archivo_salida = DATA_DIR / f"registros_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    insertados = 0
    errores = 0

    try:
        with archivo_salida.open('w', encoding='utf-8') as f:
            for registro in registros:
                try:
                    linea = f"{registro['uid']},{registro['user_id']},{registro['fecha_hora']},{registro['estado']},{registro['tipo']}\n"
                    f.write(linea)
                    insertados += 1
                    logger.info(f"✅ Guardado en TXT: UID={registro['uid']}, Fecha={registro['fecha_hora']}")
                except Exception as e:
                    errores += 1
                    logger.error(f"❌ Error al guardar registro en TXT: {e}")
    except Exception as e:
        logger.critical(f"🛑 No se pudo crear el archivo TXT: {e}")
        raise

    return insertados, errores

def main():
    """Función principal que orquesta el proceso completo."""
    try:
        with biometric_connection() as bio_conn:
            registros_crudos = obtener_registros_biometricos(bio_conn)

            # Procesar y filtrar registros válidos
            registros_procesados = []
            for i, registro in enumerate(registros_crudos, 1):
                resultado = procesar_registro(registro)
                if resultado:
                    registros_procesados.append(resultado)
                else:
                    logger.warning(f"⚠️ Registro {i} descartado por datos inválidos")

            logger.info(f"📋 Registros válidos para guardar: {len(registros_procesados)}/{len(registros_crudos)}")

            if not registros_procesados:
                logger.warning("⚠️ No hay registros válidos para guardar")
                return

            insertados, errores = guardar_registros_en_txt(registros_procesados)
            logger.info(f"📊 Resultado final: {insertados} guardados en TXT, {errores} errores")

    except Exception as e:
        logger.critical(f"🛑 Error crítico en el proceso principal: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    logger.info("🚀 Iniciando proceso de sincronización biométrica (modo archivo)")
    main()
    logger.info("🏁 Proceso completado")
